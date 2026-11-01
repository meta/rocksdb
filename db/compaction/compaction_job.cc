//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_job.h"

#include <algorithm>
#include <cinttypes>
#include <condition_variable>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <utility>
#include <vector>

#include "db/blob/blob_counting_iterator.h"
#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_file_builder.h"
#include "db/builder.h"
#include "db/compaction/clipping_iterator.h"
#include "db/compaction/compaction_state.h"
#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "db/history_trimming_iterator.h"
#include "db/log_writer.h"
#include "db/merge_helper.h"
#include "db/range_del_aggregator.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/sst_file_manager_impl.h"
#include "file/writable_file_writer.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "options/configurable_helper.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/options_type.h"
#include "table/format.h"
#include "table/merging_iterator.h"
#include "table/meta_blocks.h"
#include "table/table_builder.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "util/defer.h"
#include "util/hash_containers.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

namespace {

class SubcompactionCompletion {
 public:
  SubcompactionCompletion() = default;
  SubcompactionCompletion(const SubcompactionCompletion&) = delete;
  SubcompactionCompletion& operator=(const SubcompactionCompletion&) = delete;
  SubcompactionCompletion(SubcompactionCompletion&&) = delete;
  SubcompactionCompletion& operator=(SubcompactionCompletion&&) = delete;

  class Guard {
   public:
    explicit Guard(SubcompactionCompletion& completion)
        : completion_(&completion) {}

    ~Guard() {
      if (completion_ != nullptr) {
        completion_->Complete();
      }
    }

    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&) = delete;
    Guard(Guard&&) = delete;
    Guard& operator=(Guard&&) = delete;

    void Release() { completion_ = nullptr; }

   private:
    SubcompactionCompletion* completion_;
  };

  void Add() {
    std::lock_guard<std::mutex> lock(mutex_);
    ++remaining_;
  }

  ~SubcompactionCompletion() {
    std::unique_lock<std::mutex> lock(mutex_);
    TEST_SYNC_POINT("CompactionJob::RunSubcompactions:Wait");
    cv_.wait(lock, [&] { return remaining_ == 0; });
  }

 private:
  void Complete() {
    std::lock_guard<std::mutex> lock(mutex_);
    assert(remaining_ > 0);
    --remaining_;
    cv_.notify_one();
  }

  std::mutex mutex_;
  std::condition_variable cv_;
  size_t remaining_ = 0;
};

}  // namespace

const char* GetCompactionReasonString(CompactionReason compaction_reason) {
  switch (compaction_reason) {
    case CompactionReason::kUnknown:
      return "Unknown";
    case CompactionReason::kLevelL0FilesNum:
      return "LevelL0FilesNum";
    case CompactionReason::kLevelMaxLevelSize:
      return "LevelMaxLevelSize";
    case CompactionReason::kUniversalSizeAmplification:
      return "UniversalSizeAmplification";
    case CompactionReason::kUniversalSizeRatio:
      return "UniversalSizeRatio";
    case CompactionReason::kUniversalSortedRunNum:
      return "UniversalSortedRunNum";
    case CompactionReason::kFIFOMaxSize:
      return "FIFOMaxSize";
    case CompactionReason::kFIFOReduceNumFiles:
      return "FIFOReduceNumFiles";
    case CompactionReason::kFIFOTtl:
      return "FIFOTtl";
    case CompactionReason::kManualCompaction:
      return "ManualCompaction";
    case CompactionReason::kFilesMarkedForCompaction:
      return "FilesMarkedForCompaction";
    case CompactionReason::kBottommostFiles:
      return "BottommostFiles";
    case CompactionReason::kTtl:
      return "Ttl";
    case CompactionReason::kFlush:
      return "Flush";
    case CompactionReason::kExternalSstIngestion:
      return "ExternalSstIngestion";
    case CompactionReason::kPeriodicCompaction:
      return "PeriodicCompaction";
    case CompactionReason::kChangeTemperature:
      return "ChangeTemperature";
    case CompactionReason::kForcedBlobGC:
      return "ForcedBlobGC";
    case CompactionReason::kRoundRobinTtl:
      return "RoundRobinTtl";
    case CompactionReason::kRefitLevel:
      return "RefitLevel";
    case CompactionReason::kReadTriggered:
      return "ReadTriggered";
    case CompactionReason::kNumOfReasons:
      // fall through
    default:
      assert(false);
      return "Invalid";
  }
}

const char* GetCompactionProximalOutputRangeTypeString(
    Compaction::ProximalOutputRangeType range_type) {
  switch (range_type) {
    case Compaction::ProximalOutputRangeType::kNotSupported:
      return "NotSupported";
    case Compaction::ProximalOutputRangeType::kFullRange:
      return "FullRange";
    case Compaction::ProximalOutputRangeType::kNonLastRange:
      return "NonLastRange";
    case Compaction::ProximalOutputRangeType::kDisabled:
      return "Disabled";
    default:
      assert(false);
      return "Invalid";
  }
}

// Static constant for compaction abort flag - always false, used for
// compaction service jobs that don't support abort signaling
const std::atomic<int> CompactionJob::kCompactionAbortedFalse{0};

CompactionJob::CompactionJob(
    int job_id, Compaction* compaction, const ImmutableDBOptions& db_options,
    const MutableDBOptions& mutable_db_options, const FileOptions& file_options,
    VersionSet* versions, const std::atomic<bool>* shutting_down,
    LogBuffer* log_buffer, FSDirectory* db_directory,
    FSDirectory* output_directory, FSDirectory* blob_output_directory,
    Statistics* stats, InstrumentedMutex* db_mutex,
    ErrorHandler* db_error_handler, JobContext* job_context,
    std::shared_ptr<Cache> table_cache, EventLogger* event_logger,
    bool paranoid_file_checks, bool measure_io_stats, const std::string& dbname,
    CompactionJobStats* compaction_job_stats, Env::Priority thread_pri,
    const std::shared_ptr<IOTracer>& io_tracer,
    const std::atomic<bool>& manual_compaction_canceled,
    const std::atomic<int>& compaction_aborted, const std::string& db_id,
    const std::string& db_session_id, std::string full_history_ts_low,
    std::string trim_ts, BlobFileCompletionCallback* blob_callback,
    int* bg_compaction_scheduled, int* bg_bottom_compaction_scheduled,
    std::atomic<int>* num_running_remote_compactions)
    : compact_(new CompactionState(compaction)),
      internal_stats_(compaction->compaction_reason(), 1),
      db_options_(db_options),
      mutable_db_options_copy_(mutable_db_options),
      log_buffer_(log_buffer),
      output_directory_(output_directory),
      stats_(stats),
      bottommost_level_(false),
      write_hint_(Env::WLTH_NOT_SET),
      job_stats_(compaction_job_stats),
      job_id_(job_id),
      dbname_(dbname),
      db_id_(db_id),
      db_session_id_(db_session_id),
      file_options_(file_options),
      env_(db_options.env),
      io_tracer_(io_tracer),
      fs_(db_options.fs, io_tracer),
      file_options_for_read_(
          fs_->OptimizeForCompactionTableRead(file_options, db_options_)),
      file_options_for_compaction_input_read_(file_options_for_read_),
      versions_(versions),
      shutting_down_(shutting_down),
      manual_compaction_canceled_(manual_compaction_canceled),
      compaction_aborted_(compaction_aborted),
      db_directory_(db_directory),
      blob_output_directory_(blob_output_directory),
      db_mutex_(db_mutex),
      db_error_handler_(db_error_handler),
      // job_context cannot be nullptr, but we will assert later in the body of
      // the constructor.
      earliest_snapshot_(job_context
                             ? job_context->GetEarliestSnapshotSequence()
                             : kMaxSequenceNumber),
      job_context_(job_context),
      table_cache_(std::move(table_cache)),
      event_logger_(event_logger),
      paranoid_file_checks_(paranoid_file_checks),
      measure_io_stats_(measure_io_stats),
      thread_pri_(thread_pri),
      full_history_ts_low_(std::move(full_history_ts_low)),
      trim_ts_(std::move(trim_ts)),
      blob_callback_(blob_callback),
      extra_num_subcompaction_threads_reserved_(0),
      bg_compaction_scheduled_(bg_compaction_scheduled),
      bg_bottom_compaction_scheduled_(bg_bottom_compaction_scheduled),
      num_running_remote_compactions_(num_running_remote_compactions) {
  assert(job_stats_ != nullptr);
  assert(log_buffer_ != nullptr);
  assert(job_context);
  assert(job_context->snapshot_context_initialized);

  if (db_options_.use_direct_io_for_compaction_reads &&
      !db_options_.use_direct_reads) {
    file_options_for_compaction_input_read_.use_direct_reads = true;
  }

  ReportStartedCompaction(compaction);
  InitializeThreadStatus();
}

CompactionJob::~CompactionJob() {
  assert(compact_ == nullptr);
  ThreadStatusUtil::ResetThreadStatus();
}

void CompactionJob::InitializeThreadStatus() {
  auto* compaction = compact_->compaction;
  ThreadStatusUtil::SetEnableTracking(db_options_.enable_thread_tracking);
  ThreadStatusUtil::SetColumnFamily(compaction->column_family_data());
  ThreadStatusUtil::SetThreadOperationProperty(ThreadStatus::COMPACTION_JOB_ID,
                                               job_id_);

  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_INPUT_OUTPUT_LEVEL,
      (static_cast<uint64_t>(compact_->compaction->start_level()) << 32) +
          compact_->compaction->output_level());

  // In the current design, a CompactionJob is always created
  // for non-trivial compaction.
  assert(compaction->IsTrivialMove() == false ||
         compaction->is_manual_compaction() == true);

  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_PROP_FLAGS,
      compaction->is_manual_compaction() +
          (compaction->deletion_compaction() << 1));
  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_TOTAL_INPUT_BYTES,
      compaction->CalculateTotalInputSize());

  IOSTATS_RESET(bytes_written);
  IOSTATS_RESET(bytes_read);
  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_WRITTEN, 0);
  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_READ, 0);

  // Set the thread operation after operation properties
  // to ensure GetThreadList() can always show them all together.
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_COMPACTION);
}

void CompactionJob::ReportStartedCompaction(Compaction* compaction) {
  job_stats_->is_manual_compaction = compaction->is_manual_compaction();
  job_stats_->is_full_compaction = compaction->is_full_compaction();
  // populate compaction stats num_input_files and total_num_of_bytes
  size_t num_input_files = 0;
  for (int input_level = 0;
       input_level < static_cast<int>(compaction->num_input_levels());
       ++input_level) {
    const LevelFilesBrief* flevel = compaction->input_levels(input_level);
    num_input_files += flevel->num_files;
  }
  job_stats_->CompactionJobStats::num_input_files = num_input_files;
  job_stats_->total_input_bytes = compaction->CalculateTotalInputSize();
}

void CompactionJob::Prepare(
    std::optional<std::pair<std::optional<Slice>, std::optional<Slice>>>
        known_single_subcompact,
    const CompactionProgress& compaction_progress,
    log::Writer* compaction_progress_writer) {
  db_mutex_->AssertHeld();
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_PREPARE);

  // Generate file_levels_ for compaction before making Iterator
  auto* c = compact_->compaction;
  [[maybe_unused]] ColumnFamilyData* cfd = c->column_family_data();
  assert(cfd != nullptr);
  const VersionStorageInfo* storage_info = c->input_version()->storage_info();
  assert(storage_info);
  assert(storage_info->NumLevelFiles(compact_->compaction->level()) > 0);

  write_hint_ = storage_info->CalculateSSTWriteHint(
      c->output_level(), db_options_.calculate_sst_write_lifetime_hint_set);
  bottommost_level_ = c->bottommost_level();

  if (!known_single_subcompact.has_value() && c->ShouldFormSubcompactions()) {
    StopWatch sw(db_options_.clock, stats_, SUBCOMPACTION_SETUP_TIME);
    GenSubcompactionBoundaries();
  }
  if (boundaries_.size() >= 1) {
    assert(!known_single_subcompact.has_value());
    for (size_t i = 0; i <= boundaries_.size(); i++) {
      compact_->sub_compact_states.emplace_back(
          c, (i != 0) ? std::optional<Slice>(boundaries_[i - 1]) : std::nullopt,
          (i != boundaries_.size()) ? std::optional<Slice>(boundaries_[i])
                                    : std::nullopt,
          static_cast<uint32_t>(i));
      // assert to validate that boundaries don't have same user keys (without
      // timestamp part).
      assert(i == 0 || i == boundaries_.size() ||
             cfd->user_comparator()->CompareWithoutTimestamp(
                 boundaries_[i - 1], boundaries_[i]) < 0);
    }
    RecordInHistogram(stats_, NUM_SUBCOMPACTIONS_SCHEDULED,
                      compact_->sub_compact_states.size());
  } else {
    std::optional<Slice> start_key;
    std::optional<Slice> end_key;
    if (known_single_subcompact.has_value()) {
      start_key = known_single_subcompact.value().first;
      end_key = known_single_subcompact.value().second;
    } else {
      assert(!start_key.has_value() && !end_key.has_value());
    }
    compact_->sub_compact_states.emplace_back(c, start_key, end_key,
                                              /*sub_job_id*/ 0);
  }

  MaybeAssignCompactionProgressAndWriter(compaction_progress,
                                         compaction_progress_writer);

  // collect all seqno->time information from the input files which will be used
  // to encode seqno->time to the output files.
  SequenceNumber preserve_time_min_seqno = kMaxSequenceNumber;
  SequenceNumber preclude_last_level_min_seqno = kMaxSequenceNumber;
  uint64_t preserve_time_duration =
      MinAndMaxPreserveSeconds(c->mutable_cf_options()).max_preserve_seconds;

  if (preserve_time_duration > 0) {
    const ReadOptions read_options(Env::IOActivity::kCompaction);
    // Setup seqno_to_time_mapping_ with relevant time range.
    seqno_to_time_mapping_.SetMaxTimeSpan(preserve_time_duration);
    for (const auto& each_level : *c->inputs()) {
      for (const auto& fmd : each_level.files) {
        std::shared_ptr<const TableProperties> tp;
        Status s = c->input_version()->GetTableProperties(read_options, &tp,
                                                          fmd, nullptr);
        if (s.ok()) {
          s = seqno_to_time_mapping_.DecodeFrom(tp->seqno_to_time_mapping);
        }
        if (!s.ok()) {
          ROCKS_LOG_WARN(
              db_options_.info_log,
              "Problem reading or processing seqno-to-time mapping: %s",
              s.ToString().c_str());
        }
      }
    }

    int64_t _current_time = 0;
    Status s = db_options_.clock->GetCurrentTime(&_current_time);
    if (!s.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "Failed to get current time in compaction: Status: %s",
                     s.ToString().c_str());
      // preserve all time information
      preserve_time_min_seqno = 0;
      preclude_last_level_min_seqno = 0;
      seqno_to_time_mapping_.Enforce();
    } else {
      seqno_to_time_mapping_.Enforce(_current_time);
      seqno_to_time_mapping_.GetCurrentTieringCutoffSeqnos(
          static_cast<uint64_t>(_current_time),
          c->mutable_cf_options().preserve_internal_time_seconds,
          c->mutable_cf_options().preclude_last_level_data_seconds,
          &preserve_time_min_seqno, &preclude_last_level_min_seqno);
    }
    // For accuracy of the GetProximalSeqnoBeforeTime queries above, we only
    // limit the capacity after them.
    // Here If we set capacity to the per-SST limit, we could be throwing away
    // fidelity when a compaction output file has a narrower seqno range than
    // all the inputs. If we only limit capacity for each compaction output, we
    // could be doing a lot of unnecessary recomputation in a large compaction
    // (up to quadratic in number of files). Thus, we do soemthing in the
    // middle: enforce a resonably large constant size limit substantially
    // larger than kMaxSeqnoTimePairsPerSST.
    seqno_to_time_mapping_.SetCapacity(kMaxSeqnoToTimeEntries);
  }
#ifndef NDEBUG
  assert(preserve_time_min_seqno <= preclude_last_level_min_seqno);
  TEST_SYNC_POINT_CALLBACK(
      "CompactionJob::PrepareTimes():preclude_last_level_min_seqno",
      static_cast<void*>(&preclude_last_level_min_seqno));
  // Restore the invariant asserted above, in case it was broken under the
  // callback
  preserve_time_min_seqno =
      std::min(preclude_last_level_min_seqno, preserve_time_min_seqno);
#endif

  // For a bottommost-file compaction, which exists only because the marker
  // (VersionStorageInfo::ComputeBottommostFilesMarkedForCompaction) decided the
  // file's largest sequence number could be zeroed, honor that same
  // version-carried preserve boundary here. The marker reads DBImpl's live,
  // DB-wide seqno->time mapping, whereas the value computed above is derived
  // only from the selected file's persisted mapping, which can be sparse or
  // empty (e.g. a file written before preserve/preclude was enabled). Taking
  // the max ensures this compaction can zero at least the sequence numbers the
  // marker considered zeroable, so a marked file makes progress instead of
  // being re-marked forever (infinite compaction loop). Other compaction
  // reasons keep the per-input-file boundary so their seqno-zeroing behavior is
  // unchanged. When preserve/preclude is inactive the version value is
  // kMaxSequenceNumber, making this a no-op. A remote compaction worker does
  // not know the reason or the live mapping; it gets this boundary from the
  // primary in CompactionServiceInput.
  if (c->compaction_reason() == CompactionReason::kBottommostFiles) {
    preserve_time_min_seqno_floor_ = storage_info->GetPreserveTimeMinSeqno();
  }
  preserve_time_min_seqno =
      std::max(preserve_time_min_seqno, preserve_time_min_seqno_floor_);

  // Preserve sequence numbers for preserved write times and snapshots, though
  // the specific sequence number of the earliest snapshot can be zeroed.
  preserve_seqno_after_ =
      std::max(preserve_time_min_seqno, SequenceNumber{1}) - 1;
  preserve_seqno_after_ = std::min(preserve_seqno_after_, earliest_snapshot_);
  // If using preclude feature, also preclude snapshots from last level, just
  // because they are heuristically more likely to be accessed than non-snapshot
  // data.
  if (preclude_last_level_min_seqno < kMaxSequenceNumber &&
      earliest_snapshot_ < preclude_last_level_min_seqno) {
    preclude_last_level_min_seqno = earliest_snapshot_;
  }
  // Now combine what we would like to preclude from last level with what we
  // can safely support without dangerously moving data back up the LSM tree,
  // to get the final seqno threshold for proximal vs. last. In particular,
  // when the reserved output key range for the proximal level does not
  // include the entire last level input key range, we need to keep entries
  // already in the last level there. (Even allowing within-range entries to
  // move back up could cause problems with range tombstones. Perhaps it
  // would be better in some rare cases to keep entries in the last level
  // one-by-one rather than based on sequence number, but that would add extra
  // tracking and complexity to CompactionIterator that is probably not
  // worthwhile overall. Correctness is also more clear when splitting by
  // seqno threshold.)
  proximal_after_seqno_ = std::max(preclude_last_level_min_seqno,
                                   c->GetKeepInLastLevelThroughSeqno());

  options_file_number_ = versions_->options_file_number();

  // For remote compaction hardening: capture the current MANIFEST position as a
  // floor for the worker's recovery. manifest_file_size() is a physical byte
  // offset that VersionSet::ProcessManifestWrites records under the DB mutex
  // only after the whole version-edit batch is written and sync'd, so it is a
  // durable, complete-batch offset -- never mid-record or mid-atomic-group --
  // at or past the compaction's (already committed) input version.
  if (mutable_db_options_copy_.remote_compaction_manifest_floor) {
    min_manifest_file_number_ = versions_->manifest_file_number();
    min_manifest_file_size_ = versions_->manifest_file_size();
  }
}

void CompactionJob::MaybeAssignCompactionProgressAndWriter(
    const CompactionProgress& compaction_progress,
    log::Writer* compaction_progress_writer) {
  // LIMITATION: Only supports resuming single subcompaction for now
  if (compact_->sub_compact_states.size() != 1) {
    return;
  }

  if (!compaction_progress.empty()) {
    assert(compaction_progress.size() == 1);
    SubcompactionState* sub_compact = &compact_->sub_compact_states[0];
    const SubcompactionProgress& subcompaction_progress =
        compaction_progress[0];
    sub_compact->SetSubcompactionProgress(subcompaction_progress);
  }

  compaction_progress_writer_ = compaction_progress_writer;
}

uint64_t CompactionJob::GetSubcompactionsLimit() {
  return extra_num_subcompaction_threads_reserved_ +
         std::max(
             std::uint64_t(1),
             static_cast<uint64_t>(compact_->compaction->max_subcompactions()));
}

void CompactionJob::AcquireSubcompactionResources(
    int num_extra_required_subcompactions) {
  if (remote_compaction_executor_) {
    return;
  }
  TEST_SYNC_POINT("CompactionJob::AcquireSubcompactionResources:0");
  TEST_SYNC_POINT("CompactionJob::AcquireSubcompactionResources:1");
  int max_db_compactions =
      DBImpl::GetBGJobLimits(
          mutable_db_options_copy_.max_background_flushes,
          mutable_db_options_copy_.max_background_compactions,
          mutable_db_options_copy_.max_background_jobs,
          versions_->GetColumnFamilySet()
              ->write_controller()
              ->NeedSpeedupCompaction())
          .max_compactions;
  InstrumentedMutexLock l(db_mutex_);
  // Apply min function first since We need to compute the extra subcompaction
  // against compaction limits. And then try to reserve threads for extra
  // subcompactions. The actual number of reserved threads could be less than
  // the desired number.
  int available_bg_compactions_against_db_limit =
      std::max(max_db_compactions - *bg_compaction_scheduled_ -
                   *bg_bottom_compaction_scheduled_,
               0);
  // Reservation only supports backgrdoun threads of which the priority is
  // between BOTTOM and HIGH. Need to degrade the priority to HIGH if the
  // origin thread_pri_ is higher than that. Similar to ReleaseThreads().
  extra_num_subcompaction_threads_reserved_ =
      env_->ReserveThreads(std::min(num_extra_required_subcompactions,
                                    available_bg_compactions_against_db_limit),
                           std::min(thread_pri_, Env::Priority::HIGH));

  // Update bg_compaction_scheduled_ or bg_bottom_compaction_scheduled_
  // depending on if this compaction has the bottommost priority
  if (thread_pri_ == Env::Priority::BOTTOM) {
    *bg_bottom_compaction_scheduled_ +=
        extra_num_subcompaction_threads_reserved_;
  } else {
    *bg_compaction_scheduled_ += extra_num_subcompaction_threads_reserved_;
  }
}

void CompactionJob::ShrinkSubcompactionResources(uint64_t num_extra_resources) {
  // Do nothing when we have zero resources to shrink
  if (num_extra_resources == 0) {
    return;
  }
  db_mutex_->Lock();
  // We cannot release threads more than what we reserved before
  int extra_num_subcompaction_threads_released = env_->ReleaseThreads(
      (int)num_extra_resources, std::min(thread_pri_, Env::Priority::HIGH));
  // Update the number of reserved threads and the number of background
  // scheduled compactions for this compaction job
  extra_num_subcompaction_threads_reserved_ -=
      extra_num_subcompaction_threads_released;
  // TODO (zichen): design a test case with new subcompaction partitioning
  // when the number of actual partitions is less than the number of planned
  // partitions
  assert(extra_num_subcompaction_threads_released == (int)num_extra_resources);
  // Update bg_compaction_scheduled_ or bg_bottom_compaction_scheduled_
  // depending on if this compaction has the bottommost priority
  if (thread_pri_ == Env::Priority::BOTTOM) {
    *bg_bottom_compaction_scheduled_ -=
        extra_num_subcompaction_threads_released;
  } else {
    *bg_compaction_scheduled_ -= extra_num_subcompaction_threads_released;
  }
  db_mutex_->Unlock();
  TEST_SYNC_POINT("CompactionJob::ShrinkSubcompactionResources:0");
}

void CompactionJob::ReleaseSubcompactionResources() {
  if (extra_num_subcompaction_threads_reserved_ == 0) {
    return;
  }
  {
    InstrumentedMutexLock l(db_mutex_);
    // The number of reserved threads becomes larger than 0 only if the
    // compaction prioity is round robin and there is no sufficient
    // sub-compactions available

    // The scheduled compaction must be no less than 1 + extra number
    // subcompactions using acquired resources since this compaction job has not
    // finished yet
    assert(*bg_bottom_compaction_scheduled_ >=
               1 + extra_num_subcompaction_threads_reserved_ ||
           *bg_compaction_scheduled_ >=
               1 + extra_num_subcompaction_threads_reserved_);
  }
  ShrinkSubcompactionResources(extra_num_subcompaction_threads_reserved_);
}

void CompactionJob::GenSubcompactionBoundaries() {
  // The goal is to find some boundary keys so that we can evenly partition
  // the compaction input data into max_subcompactions ranges.
  // For every input file, we ask TableReader to estimate 128 anchor points
  // that evenly partition the input file into 128 ranges and the range
  // sizes. This can be calculated by scanning index blocks of the file.
  // Once we have the anchor points for all the input files, we merge them
  // together and try to find keys dividing ranges evenly.
  // For example, if we have two input files, and each returns following
  // ranges:
  //   File1: (a1, 1000), (b1, 1200), (c1, 1100)
  //   File2: (a2, 1100), (b2, 1000), (c2, 1000)
  // We total sort the keys to following:
  //  (a1, 1000), (a2, 1100), (b1, 1200), (b2, 1000), (c1, 1100), (c2, 1000)
  // We calculate the total size by adding up all ranges' size, which is 6400.
  // If we would like to partition into 2 subcompactions, the target of the
  // range size is 3200. Based on the size, we take "b1" as the partition key
  // since the first three ranges would hit 3200.
  //
  // Note that the ranges are actually overlapping. For example, in the example
  // above, the range ending with "b1" is overlapping with the range ending with
  // "b2". So the size 1000+1100+1200 is an underestimation of data size up to
  // "b1". In extreme cases where we only compact N L0 files, a range can
  // overlap with N-1 other ranges. Since we requested a relatively large number
  // (128) of ranges from each input files, even N range overlapping would
  // cause relatively small inaccuracy.
  ReadOptions read_options(Env::IOActivity::kCompaction);
  read_options.rate_limiter_priority = GetRateLimiterPriority();
  auto* c = compact_->compaction;
  if (c->mutable_cf_options().table_factory->Name() ==
      TableFactory::kPlainTableName()) {
    return;
  }

  if (c->max_subcompactions() <= 1 &&
      !(c->immutable_options().compaction_pri == kRoundRobin &&
        c->immutable_options().compaction_style == kCompactionStyleLevel)) {
    return;
  }
  auto* cfd = c->column_family_data();
  const Comparator* cfd_comparator = cfd->user_comparator();
  const InternalKeyComparator& icomp = cfd->internal_comparator();

  auto* v = compact_->compaction->input_version();
  int base_level = v->storage_info()->base_level();
  InstrumentedMutexUnlock unlock_guard(db_mutex_);

  uint64_t total_size = 0;
  std::vector<TableReader::Anchor> all_anchors;
  int start_lvl = c->start_level();
  int out_lvl = c->output_level();

  for (size_t lvl_idx = 0; lvl_idx < c->num_input_levels(); lvl_idx++) {
    int lvl = c->level(lvl_idx);
    if (lvl >= start_lvl && lvl <= out_lvl) {
      const LevelFilesBrief* flevel = c->input_levels(lvl_idx);
      size_t num_files = flevel->num_files;

      if (num_files == 0) {
        continue;
      }

      for (size_t i = 0; i < num_files; i++) {
        FileMetaData* f = flevel->files[i].file_metadata;
        std::vector<TableReader::Anchor> my_anchors;
        Status s = cfd->table_cache()->ApproximateKeyAnchors(
            read_options, icomp, *f, c->mutable_cf_options(), my_anchors);
        if (!s.ok() || my_anchors.empty()) {
          my_anchors.emplace_back(f->largest.user_key(), f->fd.GetFileSize());
        }
        for (auto& ac : my_anchors) {
          // Can be optimize to avoid this loop.
          total_size += ac.range_size;
        }

        all_anchors.insert(all_anchors.end(), my_anchors.begin(),
                           my_anchors.end());
      }
    }
  }
  // Here we total sort all the anchor points across all files and go through
  // them in the sorted order to find partitioning boundaries.
  // Not the most efficient implementation. A much more efficient algorithm
  // probably exists. But they are more complex. If performance turns out to
  // be a problem, we can optimize.
  std::sort(
      all_anchors.begin(), all_anchors.end(),
      [cfd_comparator](TableReader::Anchor& a, TableReader::Anchor& b) -> bool {
        return cfd_comparator->CompareWithoutTimestamp(a.user_key, b.user_key) <
               0;
      });

  // Remove duplicated entries from boundaries.
  all_anchors.erase(
      std::unique(all_anchors.begin(), all_anchors.end(),
                  [cfd_comparator](TableReader::Anchor& a,
                                   TableReader::Anchor& b) -> bool {
                    return cfd_comparator->CompareWithoutTimestamp(
                               a.user_key, b.user_key) == 0;
                  }),
      all_anchors.end());

  // Get the number of planned subcompactions, may update reserve threads
  // and update extra_num_subcompaction_threads_reserved_ for round-robin
  uint64_t num_planned_subcompactions;
  if (c->immutable_options().compaction_pri == kRoundRobin &&
      c->immutable_options().compaction_style == kCompactionStyleLevel) {
    // For round-robin compaction prioity, we need to employ more
    // subcompactions (may exceed the max_subcompaction limit). The extra
    // subcompactions will be executed using reserved threads and taken into
    // account bg_compaction_scheduled or bg_bottom_compaction_scheduled.

    // Initialized by the number of input files
    num_planned_subcompactions = static_cast<uint64_t>(c->num_input_files(0));
    uint64_t max_subcompactions_limit = GetSubcompactionsLimit();
    if (max_subcompactions_limit < num_planned_subcompactions) {
      // Assert two pointers are not empty so that we can use extra
      // subcompactions against db compaction limits
      assert(bg_bottom_compaction_scheduled_ != nullptr);
      assert(bg_compaction_scheduled_ != nullptr);
      // Reserve resources when max_subcompaction is not sufficient
      AcquireSubcompactionResources(
          (int)(num_planned_subcompactions - max_subcompactions_limit));
      // Subcompactions limit changes after acquiring additional resources.
      // Need to call GetSubcompactionsLimit() again to update the number
      // of planned subcompactions
      num_planned_subcompactions =
          std::min(num_planned_subcompactions, GetSubcompactionsLimit());
    } else {
      num_planned_subcompactions = max_subcompactions_limit;
    }
  } else {
    num_planned_subcompactions = GetSubcompactionsLimit();
  }

  TEST_SYNC_POINT_CALLBACK("CompactionJob::GenSubcompactionBoundaries:0",
                           &num_planned_subcompactions);
  if (num_planned_subcompactions == 1) {
    return;
  }

  // Group the ranges into subcompactions
  uint64_t target_range_size = std::max(
      total_size / num_planned_subcompactions,
      MaxFileSizeForLevel(
          c->mutable_cf_options(), out_lvl,
          c->immutable_options().compaction_style, base_level,
          c->immutable_options().level_compaction_dynamic_level_bytes));

  if (target_range_size >= total_size) {
    return;
  }

  uint64_t next_threshold = target_range_size;
  uint64_t cumulative_size = 0;
  uint64_t num_actual_subcompactions = 1U;
  for (TableReader::Anchor& anchor : all_anchors) {
    cumulative_size += anchor.range_size;
    if (cumulative_size > next_threshold) {
      next_threshold += target_range_size;
      num_actual_subcompactions++;
      boundaries_.push_back(anchor.user_key);
    }
    if (num_actual_subcompactions == num_planned_subcompactions) {
      break;
    }
  }
  TEST_SYNC_POINT_CALLBACK("CompactionJob::GenSubcompactionBoundaries:1",
                           &num_actual_subcompactions);
  // Shrink extra subcompactions resources when extra resrouces are acquired
  ShrinkSubcompactionResources(
      std::min((int)(num_planned_subcompactions - num_actual_subcompactions),
               extra_num_subcompaction_threads_reserved_));
}

void CompactionJob::InitializeCompactionRun() {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_RUN);
  TEST_SYNC_POINT("CompactionJob::Run():Start");
  log_buffer_->FlushBufferToLog();
  LogCompaction();
}

void CompactionJob::RunSubcompactions() {
  TEST_SYNC_POINT("CompactionJob::RunSubcompactions:BeforeStart");
  const size_t num_threads = compact_->sub_compact_states.size();
  assert(num_threads > 0);
  compact_->compaction->GetOrInitInputTableProperties();

  if (remote_compaction_executor_) {
    const auto run_subcompaction = [this](SubcompactionState* sub_compact) {
      try {
        ProcessKeyValueCompaction(sub_compact);
      } catch (...) {
        sub_compact->status = Status::Aborted();
      }
    };
    SubcompactionCompletion completion;
    for (size_t i = 1; i < num_threads; ++i) {
      try {
        completion.Add();
        SubcompactionCompletion::Guard submission(completion);
        remote_compaction_executor_([&, i] {
          SubcompactionCompletion::Guard completed(completion);
          run_subcompaction(&compact_->sub_compact_states[i]);
        });
        submission.Release();
      } catch (...) {
        compact_->sub_compact_states[0].status = Status::Aborted();
        for (size_t j = i; j < num_threads; ++j) {
          compact_->sub_compact_states[j].status = Status::Aborted();
        }
        break;
      }
    }
    if (compact_->sub_compact_states[0].status.ok()) {
      run_subcompaction(compact_->sub_compact_states.data());
    }
  } else {
    // Launch a thread for each of subcompactions 1...num_threads-1
    std::vector<port::Thread> thread_pool;
    thread_pool.reserve(num_threads - 1);
    for (size_t i = 1; i < compact_->sub_compact_states.size(); i++) {
      thread_pool.emplace_back(&CompactionJob::ProcessKeyValueCompaction, this,
                               &compact_->sub_compact_states[i]);
    }

    // Always schedule the first subcompaction (whether or not there are also
    // others) in the current thread to be efficient with resources
    ProcessKeyValueCompaction(compact_->sub_compact_states.data());

    // Wait for all other threads (if there are any) to finish execution
    for (auto& thread : thread_pool) {
      thread.join();
    }
  }
  RecordRemoteCompactionInputBlobFileCount();
  RemoveEmptyOutputs();

  ReleaseSubcompactionResources();
  TEST_SYNC_POINT("CompactionJob::ReleaseSubcompactionResources");
}

void CompactionJob::RecordRemoteCompactionInputBlobFileCount() {
  const uint64_t oldest_blob_file_number =
      remote_compaction_min_blob_file_number_.load(std::memory_order_relaxed);
  if (oldest_blob_file_number == std::numeric_limits<uint64_t>::max()) {
    return;
  }

  uint64_t blob_file_count = 0;
  const VersionStorageInfo* const storage_info =
      compact_->compaction->input_version()->storage_info();
  for (const auto& blob_file : storage_info->GetBlobFiles()) {
    if (blob_file->GetBlobFileNumber() >= oldest_blob_file_number) {
      ++blob_file_count;
    }
  }
  RecordTick(stats_, REMOTE_COMPACT_INPUT_BLOB_FILE_COUNT, blob_file_count);
}

void CompactionJob::UpdateTimingStats(uint64_t start_micros) {
  internal_stats_.SetMicros(db_options_.clock->NowMicros() - start_micros);

  for (auto& state : compact_->sub_compact_states) {
    internal_stats_.AddCpuMicros(state.compaction_job_stats.cpu_micros);
  }

  RecordTimeToHistogram(stats_, COMPACTION_TIME,
                        internal_stats_.output_level_stats.micros);
  RecordTimeToHistogram(stats_, COMPACTION_CPU_TIME,
                        internal_stats_.output_level_stats.cpu_micros);
}

void CompactionJob::RemoveEmptyOutputs() {
  for (auto& state : compact_->sub_compact_states) {
    state.RemoveLastEmptyOutput();
  }
}

void CompactionJob::CleanupAbortedSubcompactions() {
  ColumnFamilyData* cfd = compact_->compaction->column_family_data();

  uint64_t total_sst_files_deleted = 0;
  uint64_t total_blob_files_deleted = 0;

  // Track the first file deletion error to report at the end
  Status first_error;
  int deletion_errors = 0;

  // Mark all subcompactions as aborted and delete their output files
  for (auto& sub_compact : compact_->sub_compact_states) {
    // Mark this subcompaction as aborted
    sub_compact.status =
        Status::Incomplete(Status::SubCode::kCompactionAborted);

    // Delete all files (SST and blob) tracked during compaction.
    // GetOutputFilePaths() contains ALL file paths created, including
    // in-progress files that may have been removed from outputs_ or
    // blob_file_additions_.
    for (const bool is_proximal_level : {false, true}) {
      if (is_proximal_level &&
          !compact_->compaction->SupportsPerKeyPlacement()) {
        continue;
      }
      for (const std::string& file_path :
           sub_compact.Outputs(is_proximal_level)->GetOutputFilePaths()) {
        Status s = env_->DeleteFile(file_path);
        if (s.ok()) {
          // Count SST vs blob files by checking extension
          if (file_path.find(".sst") != std::string::npos) {
            total_sst_files_deleted++;
          } else if (file_path.find(".blob") != std::string::npos) {
            total_blob_files_deleted++;
          }
        } else if (!s.IsNotFound()) {
          if (first_error.ok()) {
            first_error = s;
          }
          deletion_errors++;
        }
      }
    }
    sub_compact.CleanupOutputs();
  }

  if (stats_) {
    RecordTick(stats_, COMPACTION_ABORTED);
  }

  ROCKS_LOG_INFO(db_options_.info_log,
                 "[%s] [JOB %d] Compaction aborted: deleted %" PRIu64
                 " SST files and %" PRIu64 " blob files",
                 cfd->GetName().c_str(), job_id_, total_sst_files_deleted,
                 total_blob_files_deleted);

  if (!first_error.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "[%s] [JOB %d] Cleanup completed with %d file deletion "
                    "errors. First error: %s",
                    cfd->GetName().c_str(), job_id_, deletion_errors,
                    first_error.ToString().c_str());
  }
}

bool CompactionJob::HasNewBlobFiles() const {
  for (const auto& state : compact_->sub_compact_states) {
    if (state.Current().HasBlobFileAdditions()) {
      return true;
    }
  }
  return false;
}

Status CompactionJob::CollectSubcompactionErrors() {
  Status status;
  IOStatus io_s;

  for (const auto& state : compact_->sub_compact_states) {
    if (!state.status.ok()) {
      status = state.status;
      io_s = state.io_status;
      break;
    }
  }

  if (io_status_.ok()) {
    io_status_ = io_s;
  }

  return status;
}

Status CompactionJob::SyncOutputDirectories() {
  Status status;
  IOStatus io_s;
  constexpr IODebugContext* dbg = nullptr;
  const bool wrote_new_blob_files = HasNewBlobFiles();
  if (output_directory_) {
    io_s = output_directory_->FsyncWithDirOptions(
        IOOptions(), dbg,
        DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
  }

  if (io_s.ok() && wrote_new_blob_files && blob_output_directory_ &&
      blob_output_directory_ != output_directory_) {
    io_s = blob_output_directory_->FsyncWithDirOptions(
        IOOptions(), dbg,
        DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
  }

  if (io_status_.ok()) {
    io_status_ = io_s;
  }
  if (status.ok()) {
    status = io_s;
  }

  return status;
}

Status CompactionJob::VerifyOutputFiles() {
  Status status;
  std::vector<port::Thread> thread_pool;
  ColumnFamilyData* cfd = compact_->compaction->column_family_data();
  VerifyOutputFlags verify_output_flags =
      compact_->compaction->mutable_cf_options().verify_output_flags;

  // For backward compatibility
  if (paranoid_file_checks_) {
    verify_output_flags |= VerifyOutputFlags::kVerifyIteration;
    verify_output_flags |= VerifyOutputFlags::kEnableForLocalCompaction;
    verify_output_flags |= VerifyOutputFlags::kEnableForRemoteCompaction;
  }

  auto verify_table = [&](SubcompactionState& subcompaction_state) {
    // Collect file open metadata during verification when fast_sst_open
    // is enabled, keyed by file number.
    UnorderedMap<uint64_t, std::string> file_open_metadata_map;

    for (const auto& output_file : subcompaction_state.GetOutputs()) {
      // Verify that the table is usable
      // We set for_compaction to false and don't
      // OptimizeForCompactionTableRead here because this is a special case
      // after we finish the table building No matter whether
      // use_direct_io_for_flush_and_compaction is true, we will regard this
      // verification as user reads since the goal is to cache it here for
      // further user reads
      ReadOptions verification_read_options(Env::IOActivity::kCompaction);
      verification_read_options.verify_checksums = true;
      verification_read_options.readahead_size =
          file_options_for_read_.compaction_readahead_size;

      std::unique_ptr<TableReader> table_reader_guard;
      TableReader* table_reader_ptr = table_reader_guard.get();
      verification_read_options.rate_limiter_priority =
          GetRateLimiterPriority();
      std::string file_open_metadata;
      std::string* file_open_metadata_ptr =
          mutable_db_options_copy_.fast_sst_open ? &file_open_metadata
                                                 : nullptr;
      InternalIterator* iter = cfd->table_cache()->NewIterator(
          verification_read_options, file_options_, cfd->internal_comparator(),
          output_file.meta,
          /*range_del_agg=*/nullptr, compact_->compaction->mutable_cf_options(),
          /*table_reader_ptr=*/&table_reader_ptr,
          cfd->internal_stats()->GetFileReadHist(
              compact_->compaction->output_level()),
          TableReaderCaller::kCompactionRefill, /*arena=*/nullptr,
          /*skip_filters=*/false, compact_->compaction->output_level(),
          MaxFileSizeForL0MetaPin(compact_->compaction->mutable_cf_options()),
          /*smallest_compaction_key=*/nullptr,
          /*largest_compaction_key=*/nullptr,
          /*allow_unprepared_value=*/false,
          /*range_del_read_seqno=*/nullptr,
          /*range_del_iter=*/nullptr,
          /*maybe_pin_table_handle=*/false, file_open_metadata_ptr);
      auto s = iter->status();
      if (s.ok()) {
        // Check for remote/local compaction and verify_output_flags flags
        const bool should_verify =
            (subcompaction_state.compaction_job_stats.is_remote_compaction &&
             !!(verify_output_flags &
                VerifyOutputFlags::kEnableForRemoteCompaction)) ||
            (!subcompaction_state.compaction_job_stats.is_remote_compaction &&
             !!(verify_output_flags &
                VerifyOutputFlags::kEnableForLocalCompaction));

        if (should_verify) {
          const bool should_verify_block_checksum =
              !!(verify_output_flags & VerifyOutputFlags::kVerifyBlockChecksum);
          const bool should_verify_iteration =
              !!(verify_output_flags & VerifyOutputFlags::kVerifyIteration);
          const bool should_verify_file_checksum =
              !!(verify_output_flags &
                 VerifyOutputFlags::kVerifyFileChecksum) &&
              db_options_.file_checksum_gen_factory != nullptr &&
              output_file.meta.file_checksum != kUnknownFileChecksum;
          if (should_verify_block_checksum) {
            assert(table_reader_ptr != nullptr);
            // If verifying iteration as well, verify meta blocks here only to
            // avoid redundant checks on data blocks
            s = table_reader_ptr->VerifyChecksum(
                verification_read_options, TableReaderCaller::kCompaction,
                /*meta_blocks_only=*/should_verify_iteration);
          }
          if (s.ok() && should_verify_iteration) {
            OutputValidator validator(cfd->internal_comparator(),
                                      /*_enable_hash=*/true);
            for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
              s = validator.Add(iter->key(), iter->value());
              if (!s.ok()) {
                break;
              }
            }
            if (s.ok()) {
              s = iter->status();
            }
            if (s.ok() && !validator.CompareValidator(output_file.validator)) {
              s = Status::Corruption(
                  "Key-value checksum of compaction output doesn't match what "
                  "was computed when written");
            }
          }
          if (s.ok() && should_verify_file_checksum) {
            std::string file_checksum;
            std::string file_checksum_func_name;
            std::string fname =
                GetTableFileName(output_file.meta.fd.GetNumber());
            s = GenerateOneFileChecksum(
                fs_.get(), fname, db_options_.file_checksum_gen_factory.get(),
                output_file.meta.file_checksum_func_name, &file_checksum,
                &file_checksum_func_name,
                verification_read_options.readahead_size,
                db_options_.allow_mmap_reads, io_tracer_,
                db_options_.rate_limiter.get(), verification_read_options,
                stats_, db_options_.clock, file_options_for_read_);
            if (s.ok() && file_checksum != output_file.meta.file_checksum) {
              s = Status::Corruption(
                  "File checksum mismatch for compaction output file " + fname);
            }
          }
        }
      }

      delete iter;

      if (!s.ok()) {
        subcompaction_state.status = s;
        break;
      }

      if (!file_open_metadata.empty()) {
        file_open_metadata_map[output_file.meta.fd.GetNumber()] =
            std::move(file_open_metadata);
      }
    }

    // Apply collected file open metadata to mutable outputs
    if (!file_open_metadata_map.empty()) {
      auto apply_metadata =
          [&file_open_metadata_map](
              std::vector<CompactionOutputs::Output>& outputs) {
            for (auto& output : outputs) {
              auto it = file_open_metadata_map.find(output.meta.fd.GetNumber());
              if (it != file_open_metadata_map.end()) {
                output.meta.file_open_metadata = std::move(it->second);
              }
            }
          };
      apply_metadata(subcompaction_state.GetMutableCompactionOutputs());
      apply_metadata(subcompaction_state.GetMutableProximalOutputs());
    }
  };
  for (size_t i = 1; i < compact_->sub_compact_states.size(); i++) {
    thread_pool.emplace_back(verify_table,
                             std::ref(compact_->sub_compact_states[i]));
  }
  verify_table(compact_->sub_compact_states[0]);
  for (auto& thread : thread_pool) {
    thread.join();
  }

  for (const auto& state : compact_->sub_compact_states) {
    if (!state.status.ok()) {
      status = state.status;
      break;
    }
  }

  return status;
}

void CompactionJob::SetOutputTableProperties() {
  for (const auto& state : compact_->sub_compact_states) {
    for (const auto& output : state.GetOutputs()) {
      auto fn =
          TableFileName(state.compaction->immutable_options().cf_paths,
                        output.meta.fd.GetNumber(), output.meta.fd.GetPathId());
      compact_->compaction->SetOutputTableProperties(fn,
                                                     output.table_properties);
    }
  }
}

void CompactionJob::AggregateSubcompactionOutputAndJobStats() {
  // Before the compaction starts, is_remote_compaction was set to true if
  // compaction_service is set. We now know whether each sub_compaction was
  // done remotely or not. Reset is_remote_compaction back to false and allow
  // AggregateCompactionStats() to set the right value.
  job_stats_->is_remote_compaction = false;

  // Finish up all bookkeeping to unify the subcompaction results.
  compact_->AggregateCompactionStats(internal_stats_, *job_stats_);
}

Status CompactionJob::VerifyCompactionRecordCounts(
    bool stats_built_from_input_table_prop, uint64_t num_input_range_del) {
  Status status;
  if (stats_built_from_input_table_prop &&
      job_stats_->has_accurate_num_input_records) {
    status = VerifyInputRecordCount(num_input_range_del);
    if (!status.ok()) {
      return status;
    }
  }

  const auto& mutable_cf_options = compact_->compaction->mutable_cf_options();
  if ((mutable_cf_options.table_factory->IsInstanceOf(
           TableFactory::kBlockBasedTableName()) ||
       mutable_cf_options.table_factory->IsInstanceOf(
           TableFactory::kPlainTableName()))) {
    status = VerifyOutputRecordCount();
    if (!status.ok()) {
      return status;
    }
  }
  return status;
}

void CompactionJob::FinalizeCompactionRun(
    const Status& input_status, bool stats_built_from_input_table_prop,
    uint64_t num_input_range_del) {
  if (stats_built_from_input_table_prop) {
    UpdateCompactionJobInputStatsFromInternalStats(internal_stats_,
                                                   num_input_range_del);
  }
  UpdateCompactionJobOutputStatsFromInternalStats(input_status,
                                                  internal_stats_);
  RecordCompactionIOStats();

  LogFlush(db_options_.info_log);
  TEST_SYNC_POINT("CompactionJob::Run():End");
  compact_->status = input_status;
  TEST_SYNC_POINT_CALLBACK("CompactionJob::Run():EndStatusSet",
                           const_cast<Status*>(&input_status));
}

Status CompactionJob::Run() {
  InitializeCompactionRun();

  const uint64_t start_micros = db_options_.clock->NowMicros();

  RunSubcompactions();

  UpdateTimingStats(start_micros);

  TEST_SYNC_POINT("CompactionJob::Run:BeforeVerify");

  Status status = CollectSubcompactionErrors();

  // If compaction was aborted or manually paused, clean up any output files
  // from completed subcompactions to prevent orphaned files on disk.
  // Skip cleanup for resumable compaction (when progress writer is set)
  // because the output files are needed for resumption.
  if ((status.IsCompactionAborted() || status.IsManualCompactionPaused()) &&
      compaction_progress_writer_ == nullptr) {
    CleanupAbortedSubcompactions();
  }

  if (status.ok()) {
    status = SyncOutputDirectories();
  }

  if (status.ok()) {
    status = VerifyOutputFiles();
  }

  TEST_SYNC_POINT_CALLBACK("CompactionJob::Run():AfterVerifyOutputFiles",
                           &status);

  if (status.ok()) {
    SetOutputTableProperties();
  }

  AggregateSubcompactionOutputAndJobStats();

  uint64_t num_input_range_del = 0;
  bool stats_built_from_input_table_prop =
      UpdateInternalStatsFromInputFiles(&num_input_range_del);

  if (status.ok()) {
    status = VerifyCompactionRecordCounts(stats_built_from_input_table_prop,
                                          num_input_range_del);
  }

  FinalizeCompactionRun(status, stats_built_from_input_table_prop,
                        num_input_range_del);

  return status;
}

Status CompactionJob::Install(bool* compaction_released) {
  assert(compact_);

  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_INSTALL);
  db_mutex_->AssertHeld();
  Status status = compact_->status;

  ColumnFamilyData* cfd = compact_->compaction->column_family_data();
  assert(cfd);

  int output_level = compact_->compaction->output_level();
  cfd->internal_stats()->AddCompactionStats(output_level, thread_pri_,
                                            internal_stats_);

  if (status.ok()) {
    status = InstallCompactionResults(compaction_released);
  }
  if (!versions_->io_status().ok()) {
    io_status_ = versions_->io_status();
  }

  VersionStorageInfo::LevelSummaryStorage tmp;
  auto vstorage = cfd->current()->storage_info();
  const auto& stats = internal_stats_.output_level_stats;

  double read_write_amp = 0.0;
  double write_amp = 0.0;
  double bytes_read_per_sec = 0;
  double bytes_written_per_sec = 0;

  const uint64_t bytes_read_non_output_and_blob =
      stats.bytes_read_non_output_levels + stats.bytes_read_blob;
  const uint64_t bytes_read_all =
      stats.bytes_read_output_level + bytes_read_non_output_and_blob;
  const uint64_t bytes_written_all =
      stats.bytes_written + stats.bytes_written_blob;

  if (bytes_read_non_output_and_blob > 0) {
    read_write_amp = (bytes_written_all + bytes_read_all) /
                     static_cast<double>(bytes_read_non_output_and_blob);
    write_amp =
        bytes_written_all / static_cast<double>(bytes_read_non_output_and_blob);
  }
  if (stats.micros > 0) {
    bytes_read_per_sec = bytes_read_all / static_cast<double>(stats.micros);
    bytes_written_per_sec =
        bytes_written_all / static_cast<double>(stats.micros);
  }

  const std::string& column_family_name = cfd->GetName();

  constexpr double kMB = 1048576.0;

  ROCKS_LOG_BUFFER(
      log_buffer_,
      "[%s] compacted to: %s, MB/sec: %.1f rd, %.1f wr, level %d, "
      "files in(%d, %d) filtered(%d, %d) out(%d +%d blob) "
      "MB in(%.1f, %.1f +%.1f blob) filtered(%.1f, %.1f) out(%.1f +%.1f blob), "
      "read-write-amplify(%.1f) write-amplify(%.1f) %s, records in: %" PRIu64
      ", records dropped: %" PRIu64 " output_compression: %s\n",
      column_family_name.c_str(), vstorage->LevelSummary(&tmp),
      bytes_read_per_sec, bytes_written_per_sec,
      compact_->compaction->output_level(),
      stats.num_input_files_in_non_output_levels,
      stats.num_input_files_in_output_level,
      stats.num_filtered_input_files_in_non_output_levels,
      stats.num_filtered_input_files_in_output_level, stats.num_output_files,
      stats.num_output_files_blob, stats.bytes_read_non_output_levels / kMB,
      stats.bytes_read_output_level / kMB, stats.bytes_read_blob / kMB,
      stats.bytes_skipped_non_output_levels / kMB,
      stats.bytes_skipped_output_level / kMB, stats.bytes_written / kMB,
      stats.bytes_written_blob / kMB, read_write_amp, write_amp,
      status.ToString().c_str(), stats.num_input_records,
      stats.num_dropped_records,
      CompressionTypeToString(compact_->compaction->output_compression())
          .c_str());

  const auto& blob_files = vstorage->GetBlobFiles();
  if (!blob_files.empty()) {
    assert(blob_files.front());
    assert(blob_files.back());

    ROCKS_LOG_BUFFER(
        log_buffer_,
        "[%s] Blob file summary: head=%" PRIu64 ", tail=%" PRIu64 "\n",
        column_family_name.c_str(), blob_files.front()->GetBlobFileNumber(),
        blob_files.back()->GetBlobFileNumber());
  }

  if (internal_stats_.has_proximal_level_output) {
    ROCKS_LOG_BUFFER(log_buffer_,
                     "[%s] has Proximal Level output: %" PRIu64
                     ", level %d, number of files: %" PRIu64
                     ", number of records: %" PRIu64,
                     column_family_name.c_str(),
                     internal_stats_.proximal_level_stats.bytes_written,
                     compact_->compaction->GetProximalLevel(),
                     internal_stats_.proximal_level_stats.num_output_files,
                     internal_stats_.proximal_level_stats.num_output_records);
  }

  TEST_SYNC_POINT_CALLBACK(
      "CompactionJob::Install:AfterUpdateCompactionJobStats", job_stats_);

  auto stream = event_logger_->LogToBuffer(log_buffer_, 8192);
  stream << "job" << job_id_ << "event" << "compaction_finished"
         << "compaction_time_micros" << stats.micros
         << "compaction_time_cpu_micros" << stats.cpu_micros << "output_level"
         << compact_->compaction->output_level() << "num_output_files"
         << stats.num_output_files << "total_output_size"
         << stats.bytes_written;

  if (stats.num_output_files_blob > 0) {
    stream << "num_blob_output_files" << stats.num_output_files_blob
           << "total_blob_output_size" << stats.bytes_written_blob;
  }

  stream << "num_input_records" << stats.num_input_records
         << "num_output_records" << stats.num_output_records
         << "num_subcompactions" << compact_->sub_compact_states.size()
         << "output_compression"
         << CompressionTypeToString(compact_->compaction->output_compression());

  stream << "num_single_delete_mismatches"
         << job_stats_->num_single_del_mismatch;
  stream << "num_single_delete_fallthrough"
         << job_stats_->num_single_del_fallthru;

  if (measure_io_stats_) {
    stream << "file_write_nanos" << job_stats_->file_write_nanos;
    stream << "file_range_sync_nanos" << job_stats_->file_range_sync_nanos;
    stream << "file_fsync_nanos" << job_stats_->file_fsync_nanos;
    stream << "file_prepare_write_nanos"
           << job_stats_->file_prepare_write_nanos;
  }

  stream << "lsm_state";
  stream.StartArray();
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    stream << vstorage->NumLevelFiles(level);
  }
  stream.EndArray();

  if (!blob_files.empty()) {
    assert(blob_files.front());
    stream << "blob_file_head" << blob_files.front()->GetBlobFileNumber();

    assert(blob_files.back());
    stream << "blob_file_tail" << blob_files.back()->GetBlobFileNumber();
  }

  if (internal_stats_.has_proximal_level_output) {
    InternalStats::CompactionStats& pl_stats =
        internal_stats_.proximal_level_stats;
    stream << "proximal_level_num_output_files" << pl_stats.num_output_files;
    stream << "proximal_level_bytes_written" << pl_stats.bytes_written;
    stream << "proximal_level_num_output_records"
           << pl_stats.num_output_records;
    stream << "proximal_level_num_output_files_blob"
           << pl_stats.num_output_files_blob;
    stream << "proximal_level_bytes_written_blob"
           << pl_stats.bytes_written_blob;
  }

  // Propagate Install failure to compact_->status so that
  // CleanupCompaction() -> SubcompactionState::Cleanup() sees the failure and
  // calls ReleaseObsolete on output files' table cache entries. Without this,
  // if Run() succeeds but InstallCompactionResults() fails, Cleanup would see
  // overall_status = OK and skip ReleaseObsolete, leaking entries for output
  // files that were never installed into any Version.
  if (!status.ok() && compact_->status.ok()) {
    compact_->status = status;
  }

  CleanupCompaction();
  return status;
}

void CompactionJob::NotifyOnSubcompactionBegin(
    SubcompactionState* sub_compact) {
  Compaction* c = compact_->compaction;

  if (db_options_.listeners.empty()) {
    return;
  }
  if (shutting_down_->load(std::memory_order_acquire)) {
    return;
  }
  if (c->is_manual_compaction() &&
      manual_compaction_canceled_.load(std::memory_order_acquire)) {
    return;
  }

  sub_compact->notify_on_subcompaction_completion = true;

  SubcompactionJobInfo info{};
  sub_compact->BuildSubcompactionJobInfo(info);
  info.job_id = static_cast<int>(job_id_);
  info.use_separate_remote_compaction_pool =
      compact_->compaction->use_separate_remote_compaction_pool();
  info.thread_id = env_->GetThreadID();

  for (const auto& listener : db_options_.listeners) {
    listener->OnSubcompactionBegin(info);
  }
  info.status.PermitUncheckedError();
}

void CompactionJob::NotifyOnSubcompactionCompleted(
    SubcompactionState* sub_compact) {
  if (db_options_.listeners.empty()) {
    return;
  }
  if (shutting_down_->load(std::memory_order_acquire)) {
    return;
  }

  if (sub_compact->notify_on_subcompaction_completion == false) {
    return;
  }
  sub_compact->notify_on_subcompaction_completion = false;

  SubcompactionJobInfo info{};
  sub_compact->BuildSubcompactionJobInfo(info);
  info.job_id = static_cast<int>(job_id_);
  info.use_separate_remote_compaction_pool =
      compact_->compaction->use_separate_remote_compaction_pool();
  info.thread_id = env_->GetThreadID();

  for (const auto& listener : db_options_.listeners) {
    listener->OnSubcompactionCompleted(info);
  }
}

bool CompactionJob::ShouldUseLocalCompaction(SubcompactionState* sub_compact) {
  if (db_options_.compaction_service) {
    CompactionServiceJobStatus comp_status =
        ProcessKeyValueCompactionWithCompactionService(sub_compact);
    if (comp_status != CompactionServiceJobStatus::kUseLocal) {
      return false;
    }
    // fallback to local compaction
    assert(comp_status == CompactionServiceJobStatus::kUseLocal);
    sub_compact->compaction_job_stats.is_remote_compaction = false;
  }
  return true;
}

CompactionJob::CompactionIOStatsSnapshot CompactionJob::InitializeIOStats() {
  CompactionIOStatsSnapshot io_stats;

  if (measure_io_stats_) {
    io_stats.prev_perf_level = GetPerfLevel();
    SetPerfLevel(PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
    io_stats.prev_write_nanos = IOSTATS(write_nanos);
    io_stats.prev_fsync_nanos = IOSTATS(fsync_nanos);
    io_stats.prev_range_sync_nanos = IOSTATS(range_sync_nanos);
    io_stats.prev_prepare_write_nanos = IOSTATS(prepare_write_nanos);
    io_stats.prev_cpu_write_nanos = IOSTATS(cpu_write_nanos);
    io_stats.prev_cpu_read_nanos = IOSTATS(cpu_read_nanos);
  }

  return io_stats;
}

Status CompactionJob::SetupAndValidateCompactionFilter(
    SubcompactionState* sub_compact,
    const CompactionFilter* configured_compaction_filter,
    const CompactionFilter*& compaction_filter,
    std::unique_ptr<CompactionFilter>& compaction_filter_from_factory) {
  compaction_filter = configured_compaction_filter;

  if (compaction_filter == nullptr) {
    compaction_filter_from_factory =
        sub_compact->compaction->CreateCompactionFilter();
    compaction_filter = compaction_filter_from_factory.get();
  }

  if (compaction_filter != nullptr && !compaction_filter->IgnoreSnapshots()) {
    return Status::NotSupported(
        "CompactionFilter::IgnoreSnapshots() = false is not supported "
        "anymore.");
  }

  return Status::OK();
}

void CompactionJob::InitializeReadOptionsAndBoundaries(
    const size_t ts_sz, ReadOptions& read_options,
    SubcompactionKeyBoundaries& boundaries) {
  read_options.verify_checksums = true;
  read_options.fill_cache = false;
  read_options.rate_limiter_priority = GetRateLimiterPriority();
  read_options.io_activity = Env::IOActivity::kCompaction;
  // Compaction iterators shouldn't be confined to a single prefix.
  // Compactions use Seek() for
  // (a) concurrent compactions,
  // (b) CompactionFilter::Decision::kRemoveAndSkipUntil.
  read_options.total_order_seek = true;

  // Remove the timestamps from boundaries because boundaries created in
  // GenSubcompactionBoundaries doesn't strip away the timestamp.
  if (boundaries.start.has_value()) {
    read_options.iterate_lower_bound = &(*boundaries.start);
    if (ts_sz > 0) {
      boundaries.start_without_ts =
          StripTimestampFromUserKey(*boundaries.start, ts_sz);
      read_options.iterate_lower_bound = &(*boundaries.start_without_ts);
    }
  }
  if (boundaries.end.has_value()) {
    read_options.iterate_upper_bound = &(*boundaries.end);
    if (ts_sz > 0) {
      boundaries.end_without_ts =
          StripTimestampFromUserKey(*boundaries.end, ts_sz);
      read_options.iterate_upper_bound = &(*boundaries.end_without_ts);
    }
  }

  if (ts_sz > 0) {
    if (ts_sz <= strlen(boundaries.kMaxTs)) {
      boundaries.ts_slice = Slice(boundaries.kMaxTs, ts_sz);
    } else {
      boundaries.max_ts = std::string(ts_sz, '\xff');
      boundaries.ts_slice = Slice(boundaries.max_ts);
    }
  }
  if (boundaries.start.has_value()) {
    boundaries.start_ikey.SetInternalKey(*boundaries.start, kMaxSequenceNumber,
                                         kValueTypeForSeek);
    if (ts_sz > 0) {
      boundaries.start_ikey.UpdateInternalKey(
          kMaxSequenceNumber, kValueTypeForSeek, &boundaries.ts_slice);
    }
    boundaries.start_internal_key = boundaries.start_ikey.GetInternalKey();
    boundaries.start_user_key = boundaries.start_ikey.GetUserKey();
  }
  if (boundaries.end.has_value()) {
    boundaries.end_ikey.SetInternalKey(*boundaries.end, kMaxSequenceNumber,
                                       kValueTypeForSeek);
    if (ts_sz > 0) {
      boundaries.end_ikey.UpdateInternalKey(
          kMaxSequenceNumber, kValueTypeForSeek, &boundaries.ts_slice);
    }
    boundaries.end_internal_key = boundaries.end_ikey.GetInternalKey();
    boundaries.end_user_key = boundaries.end_ikey.GetUserKey();
  }
}

InternalIterator* CompactionJob::CreateInputIterator(
    SubcompactionState* sub_compact, ColumnFamilyData* cfd,
    SubcompactionInternalIterators& iterators,
    SubcompactionKeyBoundaries& boundaries, ReadOptions& read_options) {
  const size_t ts_sz = cfd->user_comparator()->timestamp_size();
  InitializeReadOptionsAndBoundaries(ts_sz, read_options, boundaries);

  // This is assigned after creation of SubcompactionState to simplify that
  // creation across both CompactionJob and CompactionServiceCompactionJob
  sub_compact->AssignRangeDelAggregator(
      std::make_unique<CompactionRangeDelAggregator>(
          &cfd->internal_comparator(), job_context_->snapshot_seqs,
          &full_history_ts_low_, &trim_ts_));

  // Although the v2 aggregator is what the level iterator(s) know about,
  // the AddTombstones calls will be propagated down to the v1 aggregator.
  const bool open_ephemeral_table_reader =
      db_options_.use_direct_io_for_compaction_reads &&
      !db_options_.use_direct_reads;
  FileOptions& input_file_options =
      open_ephemeral_table_reader ? file_options_for_compaction_input_read_
                                  : file_options_for_read_;
  TEST_SYNC_POINT_CALLBACK(
      "CompactionJob::CreateInputIterator:InputFileOptions",
      &input_file_options);
  iterators.raw_input =
      std::unique_ptr<InternalIterator>(versions_->MakeInputIterator(
          read_options, sub_compact->compaction, sub_compact->RangeDelAgg(),
          input_file_options, boundaries.start, boundaries.end,
          open_ephemeral_table_reader));
  InternalIterator* input = iterators.raw_input.get();

  if (boundaries.start.has_value() || boundaries.end.has_value()) {
    iterators.clip = std::make_unique<ClippingIterator>(
        iterators.raw_input.get(),
        boundaries.start.has_value() ? &boundaries.start_internal_key : nullptr,
        boundaries.end.has_value() ? &boundaries.end_internal_key : nullptr,
        &cfd->internal_comparator());
    input = iterators.clip.get();
  }

  if (sub_compact->compaction->DoesInputReferenceBlobFiles()) {
    BlobGarbageMeter* meter = sub_compact->Current().GetBlobGarbageMeter();
    if (meter == nullptr) {
      meter = sub_compact->Current().CreateBlobGarbageMeter();
    }
    iterators.blob_counter = std::make_unique<BlobCountingIterator>(
        input, meter, compaction_progress_writer_ != nullptr);
    input = iterators.blob_counter.get();
  }

  if (ts_sz > 0 && !trim_ts_.empty()) {
    iterators.trim_history_iter = std::make_unique<HistoryTrimmingIterator>(
        input, cfd->user_comparator(), trim_ts_);
    input = iterators.trim_history_iter.get();
  }

  return input;
}

void CompactionJob::CreateBlobFileBuilder(
    SubcompactionState* sub_compact, ColumnFamilyData* cfd,
    std::unique_ptr<BlobFileBuilder>& blob_file_builder,
    const WriteOptions& write_options) {
  const auto& mutable_cf_options =
      sub_compact->compaction->mutable_cf_options();

  // TODO: BlobDB to support output_to_proximal_level compaction, which needs
  //  2 builders, so may need to move to `CompactionOutputs`
  if (mutable_cf_options.enable_blob_files &&
      sub_compact->compaction->output_level() >=
          mutable_cf_options.blob_file_starting_level) {
    blob_file_builder = std::make_unique<BlobFileBuilder>(
        [this]() { return NewBlobFileNumber(); }, fs_.get(),
        &sub_compact->compaction->immutable_options(), &mutable_cf_options,
        &file_options_, &write_options, db_id_, db_session_id_, job_id_,
        cfd->GetID(), cfd->GetName(), write_hint_, io_tracer_, blob_callback_,
        BlobFileCreationReason::kCompaction, GetBlobOutputPath(),
        sub_compact->Current().GetOutputFilePathsPtr(),
        sub_compact->Current().GetBlobFileAdditionsPtr());
  } else {
    blob_file_builder = nullptr;
  }
}

std::unique_ptr<CompactionIterator> CompactionJob::CreateCompactionIterator(
    SubcompactionState* sub_compact, ColumnFamilyData* cfd,
    InternalIterator* input, const CompactionFilter* compaction_filter,
    MergeHelper& merge, std::unique_ptr<BlobFileBuilder>& blob_file_builder,
    const WriteOptions& write_options) {
  CreateBlobFileBuilder(sub_compact, cfd, blob_file_builder, write_options);

  const std::string* const full_history_ts_low =
      full_history_ts_low_.empty() ? nullptr : &full_history_ts_low_;
  assert(job_context_);

  return std::make_unique<CompactionIterator>(
      input, cfd->user_comparator(), &merge, versions_->LastSequence(),
      &(job_context_->snapshot_seqs), earliest_snapshot_,
      job_context_->earliest_write_conflict_snapshot,
      job_context_->GetJobSnapshotSequence(), job_context_->snapshot_checker,
      env_, ShouldReportDetailedTime(env_, stats_), sub_compact->RangeDelAgg(),
      blob_file_builder.get(), db_options_.allow_data_in_errors,
      db_options_.enforce_single_del_contracts, manual_compaction_canceled_,
      sub_compact->compaction
          ->DoesInputReferenceBlobFiles() /* must_count_input_entries */,
      sub_compact->compaction, compaction_filter, shutting_down_,
      db_options_.info_log, full_history_ts_low, preserve_seqno_after_,
      nullptr /* input_version */, Env::IOActivity::kCompaction,
      GetBlobGarbageCollectionCutoffFileNumber());
}

std::pair<CompactionFileOpenFunc, CompactionFileCloseFunc>
CompactionJob::CreateFileHandlers(SubcompactionState* sub_compact,
                                  SubcompactionKeyBoundaries& boundaries,
                                  BlobFileBuilder* blob_file_builder,
                                  const BlobCountingIterator* blob_counter) {
  const CompactionFileOpenFunc open_file_func =
      [this, sub_compact](CompactionOutputs& outputs) {
        return this->OpenCompactionOutputFile(sub_compact, outputs);
      };

  const Slice* start_user_key =
      sub_compact->start.has_value() ? &boundaries.start_user_key : nullptr;
  const Slice* end_user_key =
      sub_compact->end.has_value() ? &boundaries.end_user_key : nullptr;

  const CompactionFileCloseFunc close_file_func =
      [this, sub_compact, start_user_key, end_user_key, blob_file_builder,
       blob_counter](const Status& status,
                     const ParsedInternalKey& prev_iter_output_internal_key,
                     const Slice& next_table_min_key,
                     const CompactionIterator* c_iter, bool post_prepare_retry,
                     CompactionOutputs& outputs) {
        return this->FinishCompactionOutputFile(
            status, prev_iter_output_internal_key, next_table_min_key,
            start_user_key, end_user_key, c_iter, post_prepare_retry,
            sub_compact, blob_file_builder, blob_counter, outputs);
      };

  return {open_file_func, close_file_func};
}

Status CompactionJob::ProcessKeyValue(
    SubcompactionState* sub_compact, ColumnFamilyData* cfd,
    CompactionIterator* c_iter, const CompactionFileOpenFunc& open_file_func,
    const CompactionFileCloseFunc& close_file_func,
    bool defer_blob_output_preparation, uint64_t& prev_cpu_micros) {
  // Cron interval for periodic operations: stats update, abort check,
  // and sync points. Uses 1024 (power of 2) for efficient bitwise check.
  const uint64_t kCronEveryMask = (1 << 10) - 1;
  [[maybe_unused]] const std::optional<const Slice> end = sub_compact->end;

  // Check for abort signal before starting key processing. The DB-wide
  // (AbortAllCompactions) and per-CF (AbortCompactions) counters compose here;
  // either aborts this job. See DB::AbortCompactions() in db.h for how the
  // abort knobs relate to each other and to the other compaction controls.
  if (compaction_aborted_.load(std::memory_order_acquire) > 0 ||
      cfd->compaction_aborted() > 0) {
    return Status::Incomplete(Status::SubCode::kCompactionAborted);
  }

  Status status;
  IterKey prev_iter_output_key;
  ParsedInternalKey prev_iter_output_internal_key;

  TEST_SYNC_POINT_CALLBACK(
      "CompactionJob::ProcessKeyValueCompaction()::Processing",
      static_cast<void*>(const_cast<Compaction*>(sub_compact->compaction)));

  while (status.ok() && !cfd->IsDropped() && c_iter->Valid() &&
         c_iter->status().ok()) {
    assert(!end.has_value() ||
           cfd->user_comparator()->Compare(c_iter->user_key(), *end) < 0);

    const uint64_t num_records = c_iter->iter_stats().num_input_records;

    // Periodic cron operations: stats update, abort check.
    if ((num_records & kCronEveryMask) == kCronEveryMask) {
      // Check for abort signal periodically
      if (compaction_aborted_.load(std::memory_order_acquire) > 0 ||
          cfd->compaction_aborted() > 0) {
        status = Status::Incomplete(Status::SubCode::kCompactionAborted);
        break;
      }

      UpdateSubcompactionJobStatsIncrementally(
          c_iter, &sub_compact->compaction_job_stats,
          db_options_.clock->CPUMicros(), prev_cpu_micros);
    }

    const auto& ikey = c_iter->ikey();
    bool use_proximal_output = ikey.sequence > proximal_after_seqno_;

#ifndef NDEBUG
    if (sub_compact->compaction->SupportsPerKeyPlacement()) {
      PerKeyPlacementContext context(sub_compact->compaction->output_level(),
                                     ikey.user_key, c_iter->value(),
                                     ikey.sequence, use_proximal_output);
      TEST_SYNC_POINT_CALLBACK("CompactionIterator::PrepareOutput.context",
                               &context);
      if (use_proximal_output) {
        // Verify that entries sent to the proximal level are within the
        // allowed range (because the input key range of the last level could
        // be larger than the allowed output key range of the proximal
        // level). This check uses user keys (ignores sequence numbers) because
        // compaction boundaries are a "clean cut" between user keys (see
        // CompactionPicker::ExpandInputsToCleanCut()), which is especially
        // important when preferred sequence numbers has been swapped in for
        // kTypeValuePreferredSeqno / TimedPut.
        sub_compact->compaction->TEST_AssertWithinProximalLevelOutputRange(
            c_iter->user_key());
      }
    } else {
      assert(proximal_after_seqno_ == kMaxSequenceNumber);
      assert(!use_proximal_output);
    }
#endif  // NDEBUG

    // Add current compaction_iterator key to target compaction output, if the
    // output file needs to be close or open, it will call the `open_file_func`
    // and `close_file_func`.
    // TODO: it would be better to have the compaction file open/close moved
    // into `CompactionOutputs` which has the output file information.
    status = sub_compact->AddToOutput(*c_iter, use_proximal_output,
                                      open_file_func, close_file_func,
                                      prev_iter_output_internal_key);
    if (!status.ok()) {
      break;
    }
    TEST_SYNC_POINT_CALLBACK("CompactionJob::Run():PausingManualCompaction:2",
                             static_cast<void*>(const_cast<std::atomic<bool>*>(
                                 &manual_compaction_canceled_)));

    prev_iter_output_key.SetInternalKey(c_iter->key(),
                                        &prev_iter_output_internal_key);
    prev_iter_output_internal_key.sequence = ikey.sequence;
    prev_iter_output_internal_key.type = ikey.type;
    if (defer_blob_output_preparation) {
      c_iter->NextWithoutPreparingOutput();
    } else {
      c_iter->Next();
    }

#ifndef NDEBUG
    bool stop = false;
    TEST_SYNC_POINT_CALLBACK("CompactionJob::ProcessKeyValueCompaction()::stop",
                             static_cast<void*>(&stop));
    if (stop) {
      break;
    }
#endif  // NDEBUG
  }

  return status;
}

void CompactionJob::UpdateSubcompactionJobStatsIncrementally(
    CompactionIterator* c_iter, CompactionJobStats* compaction_job_stats,
    uint64_t cur_cpu_micros, uint64_t& prev_cpu_micros) {
  RecordDroppedKeys(c_iter->iter_stats(), compaction_job_stats);
  c_iter->ResetRecordCounts();
  RecordCompactionIOStats();

  assert(cur_cpu_micros >= prev_cpu_micros);
  RecordTick(stats_, COMPACTION_CPU_TOTAL_TIME,
             cur_cpu_micros - prev_cpu_micros);
  prev_cpu_micros = cur_cpu_micros;
}

void CompactionJob::FinalizeSubcompactionJobStats(
    SubcompactionState* sub_compact, CompactionIterator* c_iter,
    uint64_t start_cpu_micros, uint64_t prev_cpu_micros,
    const CompactionIOStatsSnapshot& io_stats) {
  const CompactionIterationStats& c_iter_stats = c_iter->iter_stats();

  assert(!sub_compact->compaction->DoesInputReferenceBlobFiles() ||
         c_iter->HasNumInputEntryScanned());
  sub_compact->compaction_job_stats.has_accurate_num_input_records &=
      c_iter->HasNumInputEntryScanned();
  sub_compact->compaction_job_stats.num_input_records +=
      c_iter->NumInputEntryScanned();
  sub_compact->compaction_job_stats.num_blobs_read =
      c_iter_stats.num_blobs_read;
  sub_compact->compaction_job_stats.total_blob_bytes_read =
      c_iter_stats.total_blob_bytes_read;
  sub_compact->compaction_job_stats.num_input_deletion_records =
      c_iter_stats.num_input_deletion_records;
  sub_compact->compaction_job_stats.num_corrupt_keys =
      c_iter_stats.num_input_corrupt_records;
  sub_compact->compaction_job_stats.num_single_del_fallthru =
      c_iter_stats.num_single_del_fallthru;
  sub_compact->compaction_job_stats.num_single_del_mismatch =
      c_iter_stats.num_single_del_mismatch;
  sub_compact->compaction_job_stats.total_input_raw_key_bytes +=
      c_iter_stats.total_input_raw_key_bytes;
  sub_compact->compaction_job_stats.total_input_raw_value_bytes +=
      c_iter_stats.total_input_raw_value_bytes;

  RecordTick(stats_, FILTER_OPERATION_TOTAL_TIME,
             c_iter_stats.total_filter_time);

  if (c_iter_stats.num_blobs_relocated > 0) {
    RecordTick(stats_, BLOB_DB_GC_NUM_KEYS_RELOCATED,
               c_iter_stats.num_blobs_relocated);
  }
  if (c_iter_stats.total_blob_bytes_relocated > 0) {
    RecordTick(stats_, BLOB_DB_GC_BYTES_RELOCATED,
               c_iter_stats.total_blob_bytes_relocated);
  }

  uint64_t cur_cpu_micros = db_options_.clock->CPUMicros();

  // Record final compaction statistics including dropped keys, I/O stats,
  // and CPU time delta from the last periodic measurement
  UpdateSubcompactionJobStatsIncrementally(c_iter,
                                           &sub_compact->compaction_job_stats,
                                           cur_cpu_micros, prev_cpu_micros);

  // Finalize timing and I/O statistics
  sub_compact->compaction_job_stats.cpu_micros =
      cur_cpu_micros - start_cpu_micros + sub_compact->GetWorkerCPUMicros();

  if (measure_io_stats_) {
    sub_compact->compaction_job_stats.file_write_nanos +=
        IOSTATS(write_nanos) - io_stats.prev_write_nanos;
    sub_compact->compaction_job_stats.file_fsync_nanos +=
        IOSTATS(fsync_nanos) - io_stats.prev_fsync_nanos;
    sub_compact->compaction_job_stats.file_range_sync_nanos +=
        IOSTATS(range_sync_nanos) - io_stats.prev_range_sync_nanos;
    sub_compact->compaction_job_stats.file_prepare_write_nanos +=
        IOSTATS(prepare_write_nanos) - io_stats.prev_prepare_write_nanos;
    sub_compact->compaction_job_stats.cpu_micros -=
        (IOSTATS(cpu_write_nanos) - io_stats.prev_cpu_write_nanos +
         IOSTATS(cpu_read_nanos) - io_stats.prev_cpu_read_nanos) /
        1000;
    if (io_stats.prev_perf_level !=
        PerfLevel::kEnableTimeAndCPUTimeExceptForMutex) {
      SetPerfLevel(io_stats.prev_perf_level);
    }
  }
}

Status CompactionJob::FinalizeProcessKeyValueStatus(
    ColumnFamilyData* cfd, InternalIterator* input_iter,
    CompactionIterator* c_iter, Status status) {
  if (status.ok() && cfd->IsDropped()) {
    status =
        Status::ColumnFamilyDropped("Column family dropped during compaction");
  }
  if (status.ok() && shutting_down_->load(std::memory_order_relaxed)) {
    status = Status::ShutdownInProgress("Database shutdown");
  }
  if (status.ok() &&
      (manual_compaction_canceled_.load(std::memory_order_relaxed))) {
    status = Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }
  if (status.ok()) {
    status = input_iter->status();
  }
  if (status.ok()) {
    status = c_iter->status();
  }

  return status;
}

Status CompactionJob::CleanupCompactionFiles(
    SubcompactionState* sub_compact, Status status,
    const CompactionFileOpenFunc& open_file_func,
    const CompactionFileCloseFunc& close_file_func) {
  // Call FinishCompactionOutputFile() even if status is not ok: it needs to
  // close the output files. Open file function is also passed, in case there's
  // only range-dels, no file was opened, to save the range-dels, it need to
  // create a new output file.
  return sub_compact->CloseCompactionFiles(status, open_file_func,
                                           close_file_func);
}

Status CompactionJob::FinalizeBlobFiles(SubcompactionState* sub_compact,
                                        BlobFileBuilder* blob_file_builder,
                                        Status status) {
  if (blob_file_builder) {
    if (status.ok()) {
      status = blob_file_builder->Finish();
    } else {
      blob_file_builder->Abandon(status);
    }
    sub_compact->Current().UpdateBlobStats();
  }

  return status;
}

void CompactionJob::ProcessKeyValueCompaction(SubcompactionState* sub_compact) {
  TEST_SYNC_POINT("CompactionJob::ProcessKeyValueCompaction:Start");
  assert(sub_compact);
  assert(sub_compact->compaction);

  if (!ShouldUseLocalCompaction(sub_compact)) {
    return;
  }
  if (local_compaction_executor_) {
    Status status = local_compaction_executor_(
        [this, sub_compact] {
          if (thread_pri_ != Env::Priority::USER) {
            InitializeThreadStatus();
          }
          Defer reset_thread_status([this] {
            if (thread_pri_ != Env::Priority::USER) {
              ThreadStatusUtil::ResetThreadStatus();
            }
          });
          try {
            ProcessLocalKeyValueCompaction(sub_compact);
            return;
          } catch (const std::exception& e) {
            sub_compact->status =
                Status::Aborted("Local subcompaction exception", e.what());
          } catch (...) {
            sub_compact->status =
                Status::Aborted("Unknown local subcompaction exception");
          }
          sub_compact->CleanupOutputs();
          NotifyOnSubcompactionCompleted(sub_compact);
        },
        thread_pri_);
    if (!status.ok()) {
      sub_compact->status = status;
    }
  } else {
    ProcessLocalKeyValueCompaction(sub_compact);
  }
}

void CompactionJob::ProcessLocalKeyValueCompaction(
    SubcompactionState* sub_compact) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_PROCESS_KV);

  const uint64_t start_cpu_micros = db_options_.clock->CPUMicros();
  uint64_t prev_cpu_micros = start_cpu_micros;
  const CompactionIOStatsSnapshot io_stats = InitializeIOStats();
  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();
  const CompactionFilter* compaction_filter;
  std::unique_ptr<CompactionFilter> compaction_filter_from_factory = nullptr;
  Status filter_status = SetupAndValidateCompactionFilter(
      sub_compact, cfd->ioptions().compaction_filter, compaction_filter,
      compaction_filter_from_factory);
  if (!filter_status.ok()) {
    sub_compact->status = filter_status;
    return;
  }

  NotifyOnSubcompactionBegin(sub_compact);

  SubcompactionKeyBoundaries boundaries(sub_compact->start, sub_compact->end);
  SubcompactionInternalIterators iterators;
  ReadOptions read_options;
  const WriteOptions write_options(Env::IOPriority::IO_LOW,
                                   Env::IOActivity::kCompaction);

  InternalIterator* input_iter = CreateInputIterator(
      sub_compact, cfd, iterators, boundaries, read_options);

  assert(input_iter);

  Status status =
      MaybeResumeSubcompactionProgressOnInputIterator(sub_compact, input_iter);

  if (status.IsNotFound()) {
    input_iter->SeekToFirst();
  } else if (!status.ok()) {
    sub_compact->status = status;
    return;
  }

  MergeHelper merge(
      env_, cfd->user_comparator(), cfd->ioptions().merge_operator.get(),
      compaction_filter, db_options_.info_log.get(),
      false /* internal key corruption is expected */,
      job_context_->GetLatestSnapshotSequence(), job_context_->snapshot_checker,
      compact_->compaction->level(), db_options_.stats);
  std::unique_ptr<BlobFileBuilder> blob_file_builder;

  auto c_iter =
      CreateCompactionIterator(sub_compact, cfd, input_iter, compaction_filter,
                               merge, blob_file_builder, write_options);
  assert(c_iter);
  const bool defer_blob_output_preparation =
      compaction_progress_writer_ != nullptr &&
      !sub_compact->compaction->SupportsPerKeyPlacement() &&
      (blob_file_builder != nullptr ||
       sub_compact->Current().GetBlobGarbageMeter() != nullptr);
  if (defer_blob_output_preparation) {
    c_iter->SeekToFirstWithoutPreparingOutput();
  } else {
    c_iter->SeekToFirst();
  }

  TEST_SYNC_POINT("CompactionJob::Run():Inprogress");
  TEST_SYNC_POINT_CALLBACK("CompactionJob::Run():PausingManualCompaction:1",
                           static_cast<void*>(const_cast<std::atomic<bool>*>(
                               &manual_compaction_canceled_)));

  auto [open_file_func, close_file_func] =
      CreateFileHandlers(sub_compact, boundaries, blob_file_builder.get(),
                         iterators.blob_counter.get());

  status = ProcessKeyValue(sub_compact, cfd, c_iter.get(), open_file_func,
                           close_file_func, defer_blob_output_preparation,
                           prev_cpu_micros);

  status = FinalizeProcessKeyValueStatus(cfd, input_iter, c_iter.get(), status);

  FinalizeSubcompaction(sub_compact, status, open_file_func, close_file_func,
                        blob_file_builder.get(), c_iter.get(), input_iter,
                        start_cpu_micros, prev_cpu_micros, io_stats);

  NotifyOnSubcompactionCompleted(sub_compact);
}

void CompactionJob::FinalizeSubcompaction(
    SubcompactionState* sub_compact, Status status,
    const CompactionFileOpenFunc& open_file_func,
    const CompactionFileCloseFunc& close_file_func,
    BlobFileBuilder* blob_file_builder, CompactionIterator* c_iter,
    [[maybe_unused]] InternalIterator* input_iter, uint64_t start_cpu_micros,
    uint64_t prev_cpu_micros, const CompactionIOStatsSnapshot& io_stats) {
  status = CleanupCompactionFiles(sub_compact, status, open_file_func,
                                  close_file_func);
  status = FinalizeBlobFiles(sub_compact, blob_file_builder, status);

  FinalizeSubcompactionJobStats(sub_compact, c_iter, start_cpu_micros,
                                prev_cpu_micros, io_stats);

#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
  if (!status.ok()) {
    if (c_iter) {
      c_iter->status().PermitUncheckedError();
    }
    if (input_iter) {
      input_iter->status().PermitUncheckedError();
    }
  }
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED

  sub_compact->status = status;
}

uint64_t CompactionJob::GetCompactionId(SubcompactionState* sub_compact) const {
  return (uint64_t)job_id_ << 32 | sub_compact->sub_job_id;
}

void CompactionJob::RecordDroppedKeys(
    const CompactionIterationStats& c_iter_stats,
    CompactionJobStats* compaction_job_stats) {
  if (c_iter_stats.num_record_drop_user > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_USER,
               c_iter_stats.num_record_drop_user);
  }
  if (c_iter_stats.num_record_drop_hidden > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_NEWER_ENTRY,
               c_iter_stats.num_record_drop_hidden);
    if (compaction_job_stats) {
      compaction_job_stats->num_records_replaced +=
          c_iter_stats.num_record_drop_hidden;
    }
  }
  if (c_iter_stats.num_record_drop_obsolete > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_OBSOLETE,
               c_iter_stats.num_record_drop_obsolete);
    if (compaction_job_stats) {
      compaction_job_stats->num_expired_deletion_records +=
          c_iter_stats.num_record_drop_obsolete;
    }
  }
  if (c_iter_stats.num_record_drop_range_del > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_RANGE_DEL,
               c_iter_stats.num_record_drop_range_del);
  }
  if (c_iter_stats.num_range_del_drop_obsolete > 0) {
    RecordTick(stats_, COMPACTION_RANGE_DEL_DROP_OBSOLETE,
               c_iter_stats.num_range_del_drop_obsolete);
  }
  if (c_iter_stats.num_optimized_del_drop_obsolete > 0) {
    RecordTick(stats_, COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE,
               c_iter_stats.num_optimized_del_drop_obsolete);
  }
}

Status CompactionJob::FinishCompactionOutputFile(
    const Status& input_status,
    const ParsedInternalKey& prev_iter_output_internal_key,
    const Slice& next_table_min_key, const Slice* comp_start_user_key,
    const Slice* comp_end_user_key, const CompactionIterator* c_iter,
    bool post_prepare_retry, SubcompactionState* sub_compact,
    BlobFileBuilder* blob_file_builder,
    const BlobCountingIterator* blob_counter, CompactionOutputs& outputs) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_SYNC_FILE);
  assert(sub_compact != nullptr);
  assert(outputs.HasBuilder());

  FileMetaData* meta = outputs.GetMetaData();
  uint64_t output_number = meta->fd.GetNumber();
  assert(output_number != 0);

  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();
  std::string file_checksum = kUnknownFileChecksum;
  std::string file_checksum_func_name = kUnknownFileChecksumFuncName;

  // Check for iterator errors
  Status s = input_status;

  // Add range tombstones
  if (s.ok()) {
    // Inclusive lower bound, exclusive upper bound
    std::pair<SequenceNumber, SequenceNumber> keep_seqno_range{
        0, kMaxSequenceNumber};
    if (sub_compact->compaction->SupportsPerKeyPlacement()) {
      // Point entries are routed to proximal output only when their seqno is
      // strictly greater than `proximal_after_seqno_`. Range tombstones use a
      // [lower, upper) filter, so split them at the next seqno to preserve the
      // same boundary.
      SequenceNumber range_del_split_seqno = proximal_after_seqno_;
      if (range_del_split_seqno < kMaxSequenceNumber) {
        range_del_split_seqno++;
      }
      if (outputs.IsProximalLevel()) {
        keep_seqno_range.first = range_del_split_seqno;
      } else {
        keep_seqno_range.second = range_del_split_seqno;
      }
    }
    CompactionIterationStats range_del_out_stats;
    // NOTE1: Use `bottommost_level_ = true` for both bottommost and
    // output_to_proximal_level compaction here, as it's only used to decide
    // if range dels could be dropped. (Logically, we are taking a single sorted
    // run returned from CompactionIterator and physically splitting it between
    // two output levels.)
    // NOTE2: with per-key placement, range tombstones will be filtered on
    // each output level based on sequence number (traversed twice). This is
    // CPU-inefficient for a large number of range tombstones, but that would
    // be an unusual work load.
    if (sub_compact->HasRangeDel()) {
      s = outputs.AddRangeDels(*sub_compact->RangeDelAgg(), comp_start_user_key,
                               comp_end_user_key, range_del_out_stats,
                               bottommost_level_, cfd->internal_comparator(),
                               earliest_snapshot_, keep_seqno_range,
                               next_table_min_key, full_history_ts_low_);
    }
    RecordDroppedKeys(range_del_out_stats, &sub_compact->compaction_job_stats);
    TEST_SYNC_POINT("CompactionJob::FinishCompactionOutputFile1");
  }

  const uint64_t current_entries = outputs.NumEntries();

  s = outputs.Finish(s, seqno_to_time_mapping_);
  TEST_SYNC_POINT_CALLBACK(
      "CompactionJob::FinishCompactionOutputFile()::AfterFinish", &s);

  if (s.ok()) {
    // With accurate smallest and largest key, we can get a slightly more
    // accurate oldest ancestor time.
    // This makes oldest ancestor time in manifest more accurate than in
    // table properties. Not sure how to resolve it.
    if (meta->smallest.size() > 0 && meta->largest.size() > 0) {
      uint64_t refined_oldest_ancestor_time;
      Slice new_smallest = meta->smallest.user_key();
      Slice new_largest = meta->largest.user_key();
      if (!new_largest.empty() && !new_smallest.empty()) {
        refined_oldest_ancestor_time =
            sub_compact->compaction->MinInputFileOldestAncestorTime(
                &(meta->smallest), &(meta->largest));
        if (refined_oldest_ancestor_time !=
            std::numeric_limits<uint64_t>::max()) {
          meta->oldest_ancestor_time = refined_oldest_ancestor_time;
        }
      }
    }
  }

  // Finish and check for file errors
  IOStatus io_s = outputs.WriterSyncClose(s, db_options_.clock, stats_,
                                          db_options_.use_fsync);

  if (s.ok() && io_s.ok()) {
    file_checksum = meta->file_checksum;
    file_checksum_func_name = meta->file_checksum_func_name;
  }

  if (s.ok()) {
    s = io_s;
  }
  if (sub_compact->io_status.ok()) {
    sub_compact->io_status = io_s;
    // Since this error is really a copy of the
    // "normal" status, it does not also need to be checked
    sub_compact->io_status.PermitUncheckedError();
  }

  TableProperties tp;
  if (s.ok()) {
    tp = outputs.GetTableProperties();
  }
  if (s.ok() && current_entries == 0 && tp.num_range_deletions == 0) {
    // If there is nothing to output, no necessary to generate a sst file.
    // This happens when the output level is bottom level, at the same time
    // the sub_compact output nothing.
    std::string fname = GetTableFileName(meta->fd.GetNumber());

    // TODO(AR) it is not clear if there are any larger implications if
    // DeleteFile fails here
    Status ds = env_->DeleteFile(fname);
    if (!ds.ok()) {
      ROCKS_LOG_WARN(
          db_options_.info_log,
          "[%s] [JOB %d] Unable to remove SST file for table #%" PRIu64
          " at bottom level%s",
          cfd->GetName().c_str(), job_id_, output_number,
          meta->marked_for_compaction ? " (need compaction)" : "");
    }

    // Also need to remove the file from outputs, or it will be added to the
    // VersionEdit.
    outputs.RemoveLastOutput();
    meta = nullptr;
  }

  if (s.ok() && (current_entries > 0 || tp.num_range_deletions > 0)) {
    // Output to event logger and fire events.
    outputs.UpdateTableProperties();
    ROCKS_LOG_INFO(db_options_.info_log,
                   "[%s] [JOB %d] Generated table #%" PRIu64 ": %" PRIu64
                   " keys, %" PRIu64 " bytes%s, temperature: %s",
                   cfd->GetName().c_str(), job_id_, output_number,
                   current_entries, meta->fd.file_size,
                   meta->marked_for_compaction ? " (need compaction)" : "",
                   temperature_to_string[meta->temperature].c_str());
  }
  std::string fname;
  FileDescriptor output_fd;
  uint64_t oldest_blob_file_number = kInvalidBlobFileNumber;
  Status status_for_listener = s;
  if (meta != nullptr) {
    fname = GetTableFileName(meta->fd.GetNumber());
    output_fd = meta->fd;
    oldest_blob_file_number = meta->oldest_blob_file_number;
  } else {
    fname = "(nil)";
    if (s.ok()) {
      status_for_listener = Status::Aborted("Empty SST file not kept");
    }
  }
  EventHelpers::LogAndNotifyTableFileCreationFinished(
      event_logger_, cfd->ioptions().listeners, dbname_, cfd->GetName(), fname,
      job_id_, output_fd, oldest_blob_file_number, tp,
      TableFileCreationReason::kCompaction, status_for_listener, file_checksum,
      file_checksum_func_name);

  // Report new file to SstFileManagerImpl
  auto sfm =
      static_cast<SstFileManagerImpl*>(db_options_.sst_file_manager.get());
  if (sfm && meta != nullptr && meta->fd.GetPathId() == 0) {
    Status add_s = sfm->OnAddFile(fname);
    if (!add_s.ok() && s.ok()) {
      s = add_s;
    }
    if (sfm->IsMaxAllowedSpaceReached()) {
      // TODO(ajkr): should we return OK() if max space was reached by the final
      // compaction output file (similarly to how flush works when full)?
      s = Status::SpaceLimit("Max allowed space was reached");
      TEST_SYNC_POINT(
          "CompactionJob::FinishCompactionOutputFile:MaxAllowedSpaceReached");
      InstrumentedMutexLock l(db_mutex_);
      db_error_handler_->SetBGError(s, BackgroundErrorReason::kCompaction);
    }
  }

  bool should_update_progress =
      s.ok() && ShouldUpdateSubcompactionProgress(
                    sub_compact, c_iter, prev_iter_output_internal_key,
                    next_table_min_key, meta, blob_file_builder, blob_counter,
                    post_prepare_retry);
  if (post_prepare_retry) {
    TEST_SYNC_POINT_CALLBACK(
        "CompactionJob::FinishCompactionOutputFile:"
        "PostPrepareRetryShouldUpdateProgress",
        &should_update_progress);
  }
  if (should_update_progress) {
    s = sub_compact->UpdateBlobGarbageCheckpoint(
        blob_counter == nullptr || !blob_counter->TracksCurrentKeyInflow()
            ? nullptr
            : &blob_counter->GetCurrentKeyInflow());
  }
  if (s.ok() && should_update_progress && blob_file_builder != nullptr) {
    // An SST checkpoint must never reference a blob file that is still open or
    // absent from the persisted BlobFileAddition state. Closing here also
    // makes a crash before the progress record harmless: workspace cleanup
    // removes the uncheckpointed blob file and SST together.
    s = blob_file_builder->Finish();
  }
  if (s.ok() && should_update_progress) {
    UpdateSubcompactionProgress(c_iter, next_table_min_key, sub_compact);
    s = PersistSubcompactionProgress(sub_compact);
  }
  outputs.ResetBuilder();
  return s;
}

bool CompactionJob::ShouldUpdateSubcompactionProgress(
    const SubcompactionState* sub_compact, const CompactionIterator* c_iter,
    const ParsedInternalKey& prev_iter_output_internal_key,
    const Slice& next_table_min_internal_key, const FileMetaData* meta,
    const BlobFileBuilder* blob_file_builder,
    const BlobCountingIterator* blob_counter, bool post_prepare_retry) const {
  if (post_prepare_retry) {
    return false;
  }
  const auto* cfd = sub_compact->compaction->column_family_data();
  // No need to update when the progress will not get persisted
  if (compaction_progress_writer_ == nullptr) {
    return false;
  }

  bool supports_per_key_placement =
      sub_compact->compaction->SupportsPerKeyPlacement();
  TEST_SYNC_POINT_CALLBACK(
      "CompactionJob::ShouldUpdateSubcompactionProgress:"
      "SupportsPerKeyPlacement",
      &supports_per_key_placement);
  if (supports_per_key_placement &&
      (blob_file_builder != nullptr || blob_counter != nullptr)) {
    // Per-key placement prepares blob output before reaching this boundary.
    // Checkpointing would retain an unreferenced blob record when the current
    // key is processed again after resumption.
    return false;
  }

  // No need to update for a new empty output
  if (meta == nullptr) {
    return false;
  }

  // TODO(hx235): save progress even on the last output file
  if (next_table_min_internal_key.empty()) {
    return false;
  }
  assert(c_iter != nullptr);

  // LIMITATION: Persisting compaction progress with timestamp
  // is not supported since the feature of persisting timestamp of the key in
  // SST files itself is still experimental
  size_t ts_sz = cfd->user_comparator()->timestamp_size();
  if (ts_sz > 0) {
    return false;
  }

  // LIMITATION: Compaction progress persistence disabled for file boundaries
  // containing range deletions. Range deletions can span file boundaries,
  // making it difficult to ensure adjacent output tables have different user
  // keys. See the last check for why different users keys of adjacent output
  // tables are needed
  const ValueType next_table_min_internal_key_type =
      ExtractValueType(next_table_min_internal_key);
  const ValueType prev_iter_output_internal_key_type =
      prev_iter_output_internal_key.user_key.empty()
          ? ValueType::kTypeValue
          : prev_iter_output_internal_key.type;

  // Range deletes truncated to align with file boundaries may be output by the
  // compaction iterator with `ValueType::kTypeMaxValid` instead of the original
  // type.
  if ((next_table_min_internal_key_type == ValueType::kTypeRangeDeletion ||
       next_table_min_internal_key_type == ValueType::kTypeMaxValid) ||
      (prev_iter_output_internal_key_type == ValueType::kTypeRangeDeletion ||
       prev_iter_output_internal_key_type == ValueType::kTypeMaxValid)) {
    return false;
  }

  // LIMITATION: Compaction progress persistence disabled when adjacent output
  // tables share the same user key at boundaries. This ensures a simple Seek()
  // of the next key when resuming can process all versions of a user key
  const Slice next_table_min_user_key =
      ExtractUserKey(next_table_min_internal_key);
  const Slice prev_table_last_user_key =
      prev_iter_output_internal_key.user_key.empty()
          ? Slice()
          : prev_iter_output_internal_key.user_key;

  if (cfd->user_comparator()->EqualWithoutTimestamp(next_table_min_user_key,
                                                    prev_table_last_user_key)) {
    return false;
  }

  // LIMITATION: Don't save progress when the current output required looking
  // ahead or consuming an earlier input entry of the same user key. Resuming
  // through Seek() would process those entries again and double-count them.
  // A future implementation could persist enough iterator state to allow
  // checkpoints at these boundaries.
  if (c_iter->IsCurrentKeyAlreadyScanned() ||
      c_iter->HasConsumedEarlierEntriesOfCurrentKey()) {
    if (!logged_unsafe_resume_boundary_.exchange(true,
                                                 std::memory_order_relaxed)) {
      ROCKS_LOG_INFO(db_options_.info_log,
                     "[%s] [JOB %d] Skipping resumable compaction checkpoint "
                     "at an input-consumed user-key boundary",
                     cfd->GetName().c_str(), job_id_);
    }
    return false;
  }

  return true;
}

Status CompactionJob::InstallCompactionResults(bool* compaction_released) {
  assert(compact_);

  db_mutex_->AssertHeld();

  const ReadOptions read_options(Env::IOActivity::kCompaction);
  const WriteOptions write_options(Env::IOActivity::kCompaction);

  auto* compaction = compact_->compaction;
  assert(compaction);

  {
    Compaction::InputLevelSummaryBuffer inputs_summary;
    if (internal_stats_.has_proximal_level_output) {
      ROCKS_LOG_BUFFER(
          log_buffer_,
          "[%s] [JOB %d] Compacted %s => output_to_proximal_level: %" PRIu64
          " bytes + last: %" PRIu64 " bytes. Total: %" PRIu64 " bytes",
          compaction->column_family_data()->GetName().c_str(), job_id_,
          compaction->InputLevelSummary(&inputs_summary),
          internal_stats_.proximal_level_stats.bytes_written,
          internal_stats_.output_level_stats.bytes_written,
          internal_stats_.TotalBytesWritten());
    } else {
      ROCKS_LOG_BUFFER(log_buffer_,
                       "[%s] [JOB %d] Compacted %s => %" PRIu64 " bytes",
                       compaction->column_family_data()->GetName().c_str(),
                       job_id_, compaction->InputLevelSummary(&inputs_summary),
                       internal_stats_.TotalBytesWritten());
    }
  }

  VersionEdit* const edit = compaction->edit();
  assert(edit);

  struct BlobFileState {
    uint64_t total_blob_count;
    uint64_t total_blob_bytes;
    uint64_t garbage_blob_count;
    uint64_t garbage_blob_bytes;
  };
  std::unordered_map<uint64_t, BlobFileState> blob_files;
  const VersionStorageInfo* const storage_info =
      compaction->input_version()->storage_info();
  for (const auto& blob_file : storage_info->GetBlobFiles()) {
    assert(blob_file);
    blob_files.emplace(blob_file->GetBlobFileNumber(),
                       BlobFileState{blob_file->GetTotalBlobCount(),
                                     blob_file->GetTotalBlobBytes(),
                                     blob_file->GetGarbageBlobCount(),
                                     blob_file->GetGarbageBlobBytes()});
  }
  for (const auto& sub_compact : compact_->sub_compact_states) {
    for (const auto& blob : sub_compact.Current().GetBlobFileAdditions()) {
      if (!blob_files
               .emplace(blob.GetBlobFileNumber(),
                        BlobFileState{blob.GetTotalBlobCount(),
                                      blob.GetTotalBlobBytes(), 0, 0})
               .second) {
        return Status::Corruption(
            "Compaction produced duplicate blob file additions");
      }
    }
  }

  std::unordered_map<uint64_t, BlobGarbageMeter::BlobStats> blob_total_garbage;
  for (const auto& sub_compact : compact_->sub_compact_states) {
    if (sub_compact.Current().GetBlobGarbageMeter()) {
      const auto& flows = sub_compact.Current().GetBlobGarbageMeter()->flows();

      for (const auto& pair : flows) {
        const uint64_t blob_file_number = pair.first;
        const BlobGarbageMeter::BlobInOutFlow& flow = pair.second;

        assert(flow.IsValid());
        if (flow.HasGarbage()) {
          const std::unordered_map<uint64_t, BlobFileState>::const_iterator
              blob_file = blob_files.find(blob_file_number);
          if (blob_file == blob_files.end()) {
            return Status::Corruption(
                "Compaction produced garbage for an unknown blob file");
          }
          const BlobFileState& state = blob_file->second;
          if (state.garbage_blob_count > state.total_blob_count ||
              state.garbage_blob_bytes > state.total_blob_bytes) {
            return Status::Corruption(
                "Compaction encountered invalid existing blob garbage");
          }
          BlobGarbageMeter::BlobStats& aggregate =
              blob_total_garbage[blob_file_number];
          const uint64_t available_blob_count =
              state.total_blob_count - state.garbage_blob_count;
          const uint64_t available_blob_bytes =
              state.total_blob_bytes - state.garbage_blob_bytes;
          if (aggregate.GetCount() > available_blob_count ||
              aggregate.GetBytes() > available_blob_bytes ||
              flow.GetGarbageCount() >
                  available_blob_count - aggregate.GetCount() ||
              flow.GetGarbageBytes() >
                  available_blob_bytes - aggregate.GetBytes()) {
            return Status::Corruption(
                "Compaction blob garbage exceeds file totals");
          }
          aggregate.Add(flow.GetGarbageCount(), flow.GetGarbageBytes());
        }
      }
    }
  }

  // Add compaction inputs and outputs only after all blob metadata has been
  // validated across subcompactions.
  compaction->AddInputDeletions(edit);
  for (const auto& sub_compact : compact_->sub_compact_states) {
    sub_compact.AddOutputsEdit(edit);

    for (const auto& blob : sub_compact.Current().GetBlobFileAdditions()) {
      edit->AddBlobFile(blob);
    }
  }

  for (const auto& pair : blob_total_garbage) {
    const uint64_t blob_file_number = pair.first;
    const BlobGarbageMeter::BlobStats& stats = pair.second;

    edit->AddBlobFileGarbage(blob_file_number, stats.GetCount(),
                             stats.GetBytes());
  }

  if ((compaction->compaction_reason() ==
           CompactionReason::kLevelMaxLevelSize ||
       compaction->compaction_reason() == CompactionReason::kRoundRobinTtl) &&
      compaction->immutable_options().compaction_pri == kRoundRobin) {
    int start_level = compaction->start_level();
    if (start_level > 0) {
      auto vstorage = compaction->input_version()->storage_info();
      edit->AddCompactCursor(start_level,
                             vstorage->GetNextCompactCursor(
                                 start_level, compaction->num_input_files(0)));
    }
  }

  auto manifest_wcb = [&compaction, &compaction_released](const Status& s) {
    compaction->ReleaseCompactionFiles(s);
    *compaction_released = true;
  };

  Status s;
  TEST_SYNC_POINT_CALLBACK(
      "CompactionJob::InstallCompactionResults:BeforeLogAndApply", &s);
  if (s.ok()) {
    s = versions_->LogAndApply(compaction->column_family_data(), read_options,
                               write_options, edit, db_mutex_, db_directory_,
                               /*new_descriptor_log=*/false,
                               /*column_family_options=*/nullptr, manifest_wcb);
  }
  return s;
}

void CompactionJob::RecordCompactionIOStats() {
  RecordTick(stats_, COMPACT_READ_BYTES, IOSTATS(bytes_read));
  RecordTick(stats_, COMPACT_WRITE_BYTES, IOSTATS(bytes_written));
  CompactionReason compaction_reason =
      compact_->compaction->compaction_reason();
  if (compaction_reason == CompactionReason::kFilesMarkedForCompaction) {
    RecordTick(stats_, COMPACT_READ_BYTES_MARKED, IOSTATS(bytes_read));
    RecordTick(stats_, COMPACT_WRITE_BYTES_MARKED, IOSTATS(bytes_written));
  } else if (compaction_reason == CompactionReason::kPeriodicCompaction) {
    RecordTick(stats_, COMPACT_READ_BYTES_PERIODIC, IOSTATS(bytes_read));
    RecordTick(stats_, COMPACT_WRITE_BYTES_PERIODIC, IOSTATS(bytes_written));
  } else if (compaction_reason == CompactionReason::kTtl) {
    RecordTick(stats_, COMPACT_READ_BYTES_TTL, IOSTATS(bytes_read));
    RecordTick(stats_, COMPACT_WRITE_BYTES_TTL, IOSTATS(bytes_written));
  }
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_READ, IOSTATS(bytes_read));
  IOSTATS_RESET(bytes_read);
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_WRITTEN, IOSTATS(bytes_written));
  IOSTATS_RESET(bytes_written);
}

Status CompactionJob::OpenCompactionOutputFile(SubcompactionState* sub_compact,
                                               CompactionOutputs& outputs) {
  assert(sub_compact != nullptr);

  // no need to lock because VersionSet::next_file_number_ is atomic
  uint64_t file_number = versions_->NewFileNumber();
#ifndef NDEBUG
  TEST_SYNC_POINT_CALLBACK(
      "CompactionJob::OpenCompactionOutputFile::NewFileNumber", &file_number);
#endif
  std::string fname = GetTableFileName(file_number);
  // Fire events.
  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();
  EventHelpers::NotifyTableFileCreationStarted(
      cfd->ioptions().listeners, dbname_, cfd->GetName(), fname, job_id_,
      TableFileCreationReason::kCompaction);
  // Make the output file
  std::unique_ptr<FSWritableFile> writable_file;
#ifndef NDEBUG
  bool syncpoint_arg = file_options_.use_direct_writes;
  TEST_SYNC_POINT_CALLBACK("CompactionJob::OpenCompactionOutputFile",
                           &syncpoint_arg);
#endif

  // Pass temperature of the last level files to FileSystem.
  FileOptions fo_copy = file_options_;
  auto temperature =
      sub_compact->compaction->GetOutputTemperature(outputs.IsProximalLevel());
  fo_copy.temperature = temperature;
  fo_copy.open_contract = FileOpenContract::kNoReopenForWrite |
                          FileOpenContract::kNoReadersWhileOpenForWrite;
  fo_copy.write_hint = write_hint_;

  Status s;
  IOStatus io_s = NewWritableFile(fs_.get(), fname, &writable_file, fo_copy);
  s = io_s;
  if (io_s.ok()) {
    // Track the SST file path for cleanup on abort.
    outputs.AddOutputFilePath(fname);
  }
  if (sub_compact->io_status.ok()) {
    sub_compact->io_status = io_s;
    // Since this error is really a copy of the io_s that is checked below as s,
    // it does not also need to be checked.
    sub_compact->io_status.PermitUncheckedError();
  }
  if (!s.ok()) {
    ROCKS_LOG_ERROR(
        db_options_.info_log,
        "[%s] [JOB %d] OpenCompactionOutputFiles for table #%" PRIu64
        " fails at NewWritableFile with status %s",
        sub_compact->compaction->column_family_data()->GetName().c_str(),
        job_id_, file_number, s.ToString().c_str());
    LogFlush(db_options_.info_log);
    EventHelpers::LogAndNotifyTableFileCreationFinished(
        event_logger_, cfd->ioptions().listeners, dbname_, cfd->GetName(),
        fname, job_id_, FileDescriptor(), kInvalidBlobFileNumber,
        TableProperties(), TableFileCreationReason::kCompaction, s,
        kUnknownFileChecksum, kUnknownFileChecksumFuncName);
    return s;
  }

  // Try to figure out the output file's oldest ancestor time.
  int64_t temp_current_time = 0;
  auto get_time_status = db_options_.clock->GetCurrentTime(&temp_current_time);
  // Safe to proceed even if GetCurrentTime fails. So, log and proceed.
  if (!get_time_status.ok()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Failed to get current time. Status: %s",
                   get_time_status.ToString().c_str());
  }
  uint64_t current_time = static_cast<uint64_t>(temp_current_time);
  InternalKey tmp_start;
  InternalKey tmp_end;
  if (sub_compact->start.has_value()) {
    tmp_start.SetMinPossibleForUserKey(*(sub_compact->start));
  }
  if (sub_compact->end.has_value()) {
    tmp_end.SetMinPossibleForUserKey(*(sub_compact->end));
  }
  uint64_t oldest_ancestor_time =
      sub_compact->compaction->MinInputFileOldestAncestorTime(
          sub_compact->start.has_value() ? &tmp_start : nullptr,
          sub_compact->end.has_value() ? &tmp_end : nullptr);
  if (oldest_ancestor_time == std::numeric_limits<uint64_t>::max()) {
    // TODO: fix DBSSTTest.GetTotalSstFilesSize and use
    //  kUnknownOldestAncestorTime
    oldest_ancestor_time = current_time;
  }

  uint64_t newest_key_time = sub_compact->compaction->MaxInputFileNewestKeyTime(
      sub_compact->start.has_value() ? &tmp_start : nullptr,
      sub_compact->end.has_value() ? &tmp_end : nullptr);

  // Initialize a SubcompactionState::Output and add it to sub_compact->outputs
  uint64_t epoch_number = sub_compact->compaction->MinInputFileEpochNumber();
  {
    FileMetaData meta;
    meta.fd = FileDescriptor(file_number,
                             sub_compact->compaction->output_path_id(), 0);
    meta.oldest_ancestor_time = oldest_ancestor_time;
    meta.file_creation_time = current_time;
    meta.epoch_number = epoch_number;
    meta.temperature = temperature;
    assert(!db_id_.empty());
    assert(!db_session_id_.empty());
    s = GetSstInternalUniqueId(db_id_, db_session_id_, meta.fd.GetNumber(),
                               &meta.unique_id);
    if (!s.ok()) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "[%s] [JOB %d] file #%" PRIu64
                      " failed to generate unique id: %s.",
                      cfd->GetName().c_str(), job_id_, meta.fd.GetNumber(),
                      s.ToString().c_str());
      return s;
    }

    // Enable hash computation if paranoid_file_checks is on or if
    // verify_output_flags includes kVerifyIteration, so that
    // VerifyOutputFiles() can compare the hash of the written data
    // against a re-read of the output file.
    bool enable_output_hash =
        paranoid_file_checks_ ||
        !!(sub_compact->compaction->mutable_cf_options().verify_output_flags &
           VerifyOutputFlags::kVerifyIteration);
    outputs.AddOutput(std::move(meta), cfd->internal_comparator(),
                      enable_output_hash);
  }

  writable_file->SetIOPriority(GetRateLimiterPriority());
  // Subsequent attempts to override the hint via SetWriteLifeTimeHint
  // with the very same value will be ignored by the fs.
  writable_file->SetWriteLifeTimeHint(fo_copy.write_hint);
  FileTypeSet tmp_set = db_options_.checksum_handoff_file_types;
  writable_file->SetPreallocationBlockSize(static_cast<size_t>(
      sub_compact->compaction->OutputFilePreallocationSize()));
  const auto& listeners =
      sub_compact->compaction->immutable_options().listeners;
  outputs.AssignFileWriter(new WritableFileWriter(
      std::move(writable_file), fname, fo_copy, db_options_.clock, io_tracer_,
      db_options_.stats, Histograms::SST_WRITE_MICROS, listeners,
      db_options_.file_checksum_gen_factory.get(),
      tmp_set.Contains(FileType::kTableFile),
      tmp_set.Contains(FileType::kTableFile)));

  // TODO(hx235): pass in the correct `oldest_key_time` instead of `0`
  const ReadOptions read_options(Env::IOActivity::kCompaction);
  const WriteOptions write_options(Env::IOActivity::kCompaction);
  TableBuilderOptions tboptions(
      cfd->ioptions(), sub_compact->compaction->mutable_cf_options(),
      read_options, write_options, cfd->internal_comparator(),
      cfd->internal_tbl_prop_coll_factories(),
      sub_compact->compaction->output_compression(),
      sub_compact->compaction->output_compression_opts(), cfd->GetID(),
      cfd->GetName(), sub_compact->compaction->output_level(), newest_key_time,
      bottommost_level_, TableFileCreationReason::kCompaction,
      0 /* oldest_key_time */, current_time, db_id_, db_session_id_,
      sub_compact->compaction->max_output_file_size(), file_number,
      proximal_after_seqno_ /*last_level_inclusive_max_seqno_threshold*/,
      dbname_ /*db_name*/, IsRemoteCompaction() /*is_remote_compaction*/);

  outputs.NewBuilder(tboptions);

  LogFlush(db_options_.info_log);
  return s;
}

void CompactionJob::CleanupCompaction() {
  for (SubcompactionState& sub_compact : compact_->sub_compact_states) {
    sub_compact.Cleanup(table_cache_.get(), compact_->status);
  }
  delete compact_;
  compact_ = nullptr;
}

namespace {
void CopyPrefix(const Slice& src, size_t prefix_length, std::string* dst) {
  assert(prefix_length > 0);
  size_t length = src.size() > prefix_length ? prefix_length : src.size();
  dst->assign(src.data(), length);
}
}  // namespace

bool CompactionJob::UpdateInternalStatsFromInputFiles(
    uint64_t* num_input_range_del) {
  assert(compact_);

  Compaction* compaction = compact_->compaction;
  internal_stats_.output_level_stats.num_input_files_in_non_output_levels = 0;
  internal_stats_.output_level_stats.num_input_files_in_output_level = 0;

  bool has_error = false;
  const ReadOptions read_options(Env::IOActivity::kCompaction);
  const auto& input_table_properties = compaction->GetInputTableProperties();

  // Check all input files for old block-based SST format_version. Why? Old
  // block-based SST files from roughly version 5.0 to 5.18 could produce
  // inaccurate num_entries counts due to the evolution of its handling along
  // with num_range_deletions. We have to disable some paranoid checks when
  // compacting files from such an old release. However, we don't have great
  // information to identify those files, so we heuristically over-approximate
  // that set of files using
  // (a) format_version < 5, which will be true for any files from RocksDB <
  // 6.6.0 and should not be true for any recent production files
  // (b) to avoid including non-block-based SST files (which still use older
  // format_version markers, and do not support DeleteRange), we also require
  // the presence of the user property "rocksdb.block.based.table.index.type",
  // which was added in RocksDB 2.8 and is always present in block-based tables.
  for (const auto& tp_pair : input_table_properties) {
    if (tp_pair.second && tp_pair.second->format_version < 5) {
      // Check for block-based table by looking for its index type property
      const auto& user_props = tp_pair.second->user_collected_properties;
      if (user_props.find(BlockBasedTablePropertyNames::kIndexType) !=
          user_props.end()) {
        job_stats_->has_accurate_num_input_records = false;
        break;
      }
    }
  }

  for (int input_level = 0;
       input_level < static_cast<int>(compaction->num_input_levels());
       ++input_level) {
    const LevelFilesBrief* flevel = compaction->input_levels(input_level);
    size_t num_input_files = flevel->num_files;
    uint64_t* bytes_read;
    if (compaction->level(input_level) != compaction->output_level()) {
      internal_stats_.output_level_stats.num_input_files_in_non_output_levels +=
          static_cast<int>(num_input_files);
      bytes_read =
          &internal_stats_.output_level_stats.bytes_read_non_output_levels;
    } else {
      internal_stats_.output_level_stats.num_input_files_in_output_level +=
          static_cast<int>(num_input_files);
      bytes_read = &internal_stats_.output_level_stats.bytes_read_output_level;
    }
    for (size_t i = 0; i < num_input_files; ++i) {
      const FileMetaData* file_meta = flevel->files[i].file_metadata;
      *bytes_read += file_meta->fd.GetFileSize();
      uint64_t file_input_entries = file_meta->num_entries;
      uint64_t file_num_range_del = file_meta->num_range_deletions;
      if (file_input_entries == 0) {
        uint64_t file_number = file_meta->fd.GetNumber();
        // Try getting info from table property
        std::string fn = TableFileName(compaction->immutable_options().cf_paths,
                                       file_number, file_meta->fd.GetPathId());
        const auto& tp = input_table_properties.find(fn);
        if (tp != input_table_properties.end()) {
          file_input_entries = tp->second->num_entries;
          file_num_range_del = tp->second->num_range_deletions;
        } else {
          has_error = true;
        }
      }
      internal_stats_.output_level_stats.num_input_records +=
          file_input_entries;
      if (num_input_range_del) {
        *num_input_range_del += file_num_range_del;
      }
    }

    const std::vector<FileMetaData*>& filtered_flevel =
        compaction->filtered_input_levels(input_level);
    size_t num_filtered_input_files = filtered_flevel.size();
    uint64_t* bytes_skipped;
    if (compaction->level(input_level) != compaction->output_level()) {
      internal_stats_.output_level_stats
          .num_filtered_input_files_in_non_output_levels +=
          static_cast<int>(num_filtered_input_files);
      bytes_skipped =
          &internal_stats_.output_level_stats.bytes_skipped_non_output_levels;
    } else {
      internal_stats_.output_level_stats
          .num_filtered_input_files_in_output_level +=
          static_cast<int>(num_filtered_input_files);
      bytes_skipped =
          &internal_stats_.output_level_stats.bytes_skipped_output_level;
    }
    for (const FileMetaData* filtered_file_meta : filtered_flevel) {
      *bytes_skipped += filtered_file_meta->fd.GetFileSize();
    }
  }

  // TODO - find a better place to set these two
  assert(job_stats_);
  internal_stats_.output_level_stats.bytes_read_blob =
      job_stats_->total_blob_bytes_read;
  internal_stats_.output_level_stats.num_dropped_records =
      internal_stats_.DroppedRecords();
  return !has_error;
}

void CompactionJob::UpdateCompactionJobInputStatsFromInternalStats(
    const InternalStats::CompactionStatsFull& internal_stats,
    uint64_t num_input_range_del) const {
  assert(job_stats_);
  // input information
  job_stats_->total_input_bytes =
      internal_stats.output_level_stats.bytes_read_non_output_levels +
      internal_stats.output_level_stats.bytes_read_output_level;
  job_stats_->num_input_records =
      internal_stats.output_level_stats.num_input_records - num_input_range_del;
  job_stats_->num_input_files =
      internal_stats.output_level_stats.num_input_files_in_non_output_levels +
      internal_stats.output_level_stats.num_input_files_in_output_level;
  job_stats_->num_input_files_at_output_level =
      internal_stats.output_level_stats.num_input_files_in_output_level;
  job_stats_->num_filtered_input_files =
      internal_stats.output_level_stats
          .num_filtered_input_files_in_non_output_levels +
      internal_stats.output_level_stats
          .num_filtered_input_files_in_output_level;
  job_stats_->num_filtered_input_files_at_output_level =
      internal_stats.output_level_stats
          .num_filtered_input_files_in_output_level;
  job_stats_->total_skipped_input_bytes =
      internal_stats.output_level_stats.bytes_skipped_non_output_levels +
      internal_stats.output_level_stats.bytes_skipped_output_level;

  if (internal_stats.has_proximal_level_output) {
    job_stats_->total_input_bytes +=
        internal_stats.proximal_level_stats.bytes_read_non_output_levels +
        internal_stats.proximal_level_stats.bytes_read_output_level;
    job_stats_->num_input_records +=
        internal_stats.proximal_level_stats.num_input_records;
    job_stats_->num_input_files +=
        internal_stats.proximal_level_stats
            .num_input_files_in_non_output_levels +
        internal_stats.proximal_level_stats.num_input_files_in_output_level;
    job_stats_->num_input_files_at_output_level +=
        internal_stats.proximal_level_stats.num_input_files_in_output_level;
    job_stats_->num_filtered_input_files +=
        internal_stats.proximal_level_stats
            .num_filtered_input_files_in_non_output_levels +
        internal_stats.proximal_level_stats
            .num_filtered_input_files_in_output_level;
    job_stats_->num_filtered_input_files_at_output_level +=
        internal_stats.proximal_level_stats
            .num_filtered_input_files_in_output_level;
    job_stats_->total_skipped_input_bytes +=
        internal_stats.proximal_level_stats.bytes_skipped_non_output_levels +
        internal_stats.proximal_level_stats.bytes_skipped_output_level;
  }
}

void CompactionJob::UpdateCompactionJobOutputStatsFromInternalStats(
    const Status& status,
    const InternalStats::CompactionStatsFull& internal_stats) const {
  assert(job_stats_);
  job_stats_->elapsed_micros = internal_stats.output_level_stats.micros;
  job_stats_->cpu_micros = internal_stats.output_level_stats.cpu_micros;

  // output information
  job_stats_->total_output_bytes =
      internal_stats.output_level_stats.bytes_written;
  job_stats_->total_output_bytes_blob =
      internal_stats.output_level_stats.bytes_written_blob;
  job_stats_->num_output_records =
      internal_stats.output_level_stats.num_output_records;
  job_stats_->num_output_files =
      internal_stats.output_level_stats.num_output_files;
  job_stats_->num_output_files_blob =
      internal_stats.output_level_stats.num_output_files_blob;

  if (internal_stats.has_proximal_level_output) {
    job_stats_->total_output_bytes +=
        internal_stats.proximal_level_stats.bytes_written;
    job_stats_->total_output_bytes_blob +=
        internal_stats.proximal_level_stats.bytes_written_blob;
    job_stats_->num_output_records +=
        internal_stats.proximal_level_stats.num_output_records;
    job_stats_->num_output_files +=
        internal_stats.proximal_level_stats.num_output_files;
    job_stats_->num_output_files_blob +=
        internal_stats.proximal_level_stats.num_output_files_blob;
  }

  if (status.ok() && job_stats_->num_output_files > 0) {
    CopyPrefix(compact_->SmallestUserKey(),
               CompactionJobStats::kMaxPrefixLength,
               &job_stats_->smallest_output_key_prefix);
    CopyPrefix(compact_->LargestUserKey(), CompactionJobStats::kMaxPrefixLength,
               &job_stats_->largest_output_key_prefix);
  }
}

void CompactionJob::LogCompaction() {
  Compaction* compaction = compact_->compaction;
  ColumnFamilyData* cfd = compaction->column_family_data();
  // Let's check if anything will get logged. Don't prepare all the info if
  // we're not logging
  if (db_options_.info_log_level <= InfoLogLevel::INFO_LEVEL) {
    Compaction::InputLevelSummaryBuffer inputs_summary;
    ROCKS_LOG_INFO(
        db_options_.info_log, "[%s] [JOB %d] Compacting %s, score %.2f",
        cfd->GetName().c_str(), job_id_,
        compaction->InputLevelSummary(&inputs_summary), compaction->score());
    char scratch[2345];
    compaction->Summary(scratch, sizeof(scratch));
    ROCKS_LOG_INFO(db_options_.info_log, "[%s]: Compaction start summary: %s\n",
                   cfd->GetName().c_str(), scratch);
    // build event logger report
    auto stream = event_logger_->Log();
    stream << "job" << job_id_ << "event" << "compaction_started" << "cf_name"
           << cfd->GetName() << "compaction_reason"
           << GetCompactionReasonString(compaction->compaction_reason());
    for (size_t i = 0; i < compaction->num_input_levels(); ++i) {
      stream << ("files_L" + std::to_string(compaction->level(i)));
      stream.StartArray();
      for (auto f : *compaction->inputs(i)) {
        stream << f->fd.GetNumber();
      }
      stream.EndArray();
    }
    stream << "score" << compaction->score() << "input_data_size"
           << compaction->CalculateTotalInputSize() << "oldest_snapshot_seqno"
           << (job_context_->snapshot_seqs.empty()
                   ? int64_t{-1}  // Use -1 for "none"
                   : static_cast<int64_t>(
                         job_context_->GetEarliestSnapshotSequence()));
    if (compaction->SupportsPerKeyPlacement()) {
      stream << "proximal_after_seqno" << proximal_after_seqno_;
      stream << "preserve_seqno_after" << preserve_seqno_after_;
      stream << "proximal_output_level" << compaction->GetProximalLevel();
      stream << "proximal_output_range"
             << GetCompactionProximalOutputRangeTypeString(
                    compaction->GetProximalOutputRangeType());

      if (compaction->GetProximalOutputRangeType() ==
          Compaction::ProximalOutputRangeType::kDisabled) {
        ROCKS_LOG_WARN(
            db_options_.info_log,
            "[%s] [JOB %d] Proximal level output is disabled, likely "
            "because of the range conflict in the proximal level",
            cfd->GetName().c_str(), job_id_);
      }
    }
  }
}

std::string CompactionJob::GetTableFileName(uint64_t file_number) {
  return TableFileName(compact_->compaction->immutable_options().cf_paths,
                       file_number, compact_->compaction->output_path_id());
}

const std::string& CompactionJob::GetBlobOutputPath() const {
  return compact_->compaction->immutable_options().cf_paths.front().path;
}

uint64_t CompactionJob::NewBlobFileNumber() {
  return versions_->NewFileNumber();
}

uint64_t CompactionJob::GetNextBlobFileNumber() const {
  return versions_->current_next_file_number();
}

Status CompactionJob::RestoreNextBlobFileNumber(
    const SubcompactionProgress& subcompaction_progress) {
  if (!subcompaction_progress.has_blob_compaction_progress) {
    return Status::OK();
  }

  const uint64_t next_blob_file_number =
      subcompaction_progress.next_blob_file_number;
  if (next_blob_file_number == kInvalidBlobFileNumber) {
    return Status::Corruption("Invalid next blob file number in progress");
  }
  const uint64_t current_next_file_number =
      versions_->current_next_file_number();
  if (current_next_file_number < next_blob_file_number) {
    versions_->FetchAddFileNumber(next_blob_file_number -
                                  current_next_file_number);
  }
  return Status::OK();
}

Env::IOPriority CompactionJob::GetRateLimiterPriority() {
  if (versions_ && versions_->GetColumnFamilySet() &&
      versions_->GetColumnFamilySet()->write_controller()) {
    WriteController* write_controller =
        versions_->GetColumnFamilySet()->write_controller();
    if (write_controller->NeedsDelay() || write_controller->IsStopped()) {
      return Env::IO_USER;
    }
  }

  return Env::IO_LOW;
}

Status CompactionJob::ReadTablePropertiesDirectly(
    const ImmutableOptions& ioptions, const MutableCFOptions& moptions,
    const FileMetaData* file_meta, const ReadOptions& read_options,
    std::shared_ptr<const TableProperties>* tp) {
  std::unique_ptr<FSRandomAccessFile> file;
  std::string file_name = GetTableFileName(file_meta->fd.GetNumber());
  FileOptions fopts = file_options_;
  fopts.file_checksum = file_meta->file_checksum;
  fopts.file_checksum_func_name = file_meta->file_checksum_func_name;
  Status s = ioptions.fs->NewRandomAccessFile(file_name, fopts, &file,
                                              nullptr /* dbg */);
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(
          std::move(file), file_name, ioptions.clock, io_tracer_,
          ioptions.stats, Histograms::SST_READ_MICROS /* hist_type */,
          nullptr /* file_read_hist */, ioptions.rate_limiter.get(),
          ioptions.listeners));

  std::unique_ptr<TableProperties> props;

  uint64_t magic_number = kBlockBasedTableMagicNumber;

  const auto* table_factory = moptions.table_factory.get();
  if (table_factory == nullptr) {
    return Status::Incomplete("Table factory is not set");
  } else {
    const auto& table_factory_name = table_factory->Name();
    if (table_factory_name == TableFactory::kPlainTableName()) {
      magic_number = kPlainTableMagicNumber;
    } else if (table_factory_name == TableFactory::kCuckooTableName()) {
      magic_number = kCuckooTableMagicNumber;
    }
  }

  s = ReadTableProperties(file_reader.get(), file_meta->fd.GetFileSize(),
                          magic_number, ioptions, read_options, &props);
  if (!s.ok()) {
    return s;
  }

  *tp = std::move(props);
  return s;
}

Status CompactionJob::ReadOutputFilesTableProperties(
    const autovector<FileMetaData>& output_files,
    const ReadOptions& read_options,
    std::vector<std::shared_ptr<const TableProperties>>&
        output_files_table_properties,
    bool is_proximal_level) {
  assert(!output_files.empty());

  static const char* level_type =
      is_proximal_level ? "proximal output" : "output";

  output_files_table_properties.reserve(output_files.size());

  Status s;

  for (const FileMetaData& metadata : output_files) {
    std::shared_ptr<const TableProperties> tp;
    s = ReadTablePropertiesDirectly(compact_->compaction->immutable_options(),
                                    compact_->compaction->mutable_cf_options(),
                                    &metadata, read_options, &tp);
    if (!s.ok()) {
      ROCKS_LOG_ERROR(
          db_options_.info_log,
          "Failed to read table properties for %s level output file #%" PRIu64
          ": %s",
          level_type, metadata.fd.GetNumber(), s.ToString().c_str());
      return s;
    }

    if (tp == nullptr) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Empty table property for %s level output file #%" PRIu64
                      "",
                      level_type, metadata.fd.GetNumber());

      s = Status::Corruption("Empty table property for " +
                             std::string(level_type) +
                             " level output files during resuming");
      return s;
    }
    output_files_table_properties.push_back(tp);
  }
  return s;
}

void CompactionJob::RestoreCompactionOutputs(
    const ColumnFamilyData* cfd,
    const std::vector<std::shared_ptr<const TableProperties>>&
        output_files_table_properties,
    SubcompactionProgressPerLevel& subcompaction_progress_per_level,
    CompactionOutputs* outputs_to_restore) {
  assert(outputs_to_restore->GetOutputs().size() == 0);

  const auto& output_files = subcompaction_progress_per_level.GetOutputFiles();

  const bool enable_output_hash =
      paranoid_file_checks_ ||
      !!(compact_->compaction->mutable_cf_options().verify_output_flags &
         VerifyOutputFlags::kVerifyIteration);

  for (size_t i = 0; i < output_files.size(); i++) {
    FileMetaData file_copy = output_files[i];

    outputs_to_restore->AddOutput(std::move(file_copy),
                                  cfd->internal_comparator(),
                                  enable_output_hash, true /* finished */);

    outputs_to_restore->UpdateTableProperties(
        *output_files_table_properties[i]);
  }

  outputs_to_restore->SetNumOutputRecords(
      subcompaction_progress_per_level.GetNumProcessedOutputRecords());
}

Status CompactionJob::RestoreBlobCompactionProgress(
    SubcompactionState* sub_compact,
    const SubcompactionProgress& subcompaction_progress) {
  if (!subcompaction_progress.has_blob_compaction_progress) {
    return RestoreNextBlobFileNumber(subcompaction_progress);
  }

  CompactionOutputs* const outputs = sub_compact->Outputs(false);
  assert(outputs);
  assert(outputs->GetBlobFileAdditions().empty());
  *outputs->GetBlobFileAdditionsPtr() =
      subcompaction_progress.blob_file_additions;

  if (!subcompaction_progress.blob_file_garbages.empty()) {
    BlobGarbageMeter* meter = outputs->GetBlobGarbageMeter();
    if (meter == nullptr) {
      meter = outputs->CreateBlobGarbageMeter();
    }
    for (const auto& garbage : subcompaction_progress.blob_file_garbages) {
      meter->AddGarbage(garbage.GetBlobFileNumber(),
                        garbage.GetGarbageBlobCount(),
                        garbage.GetGarbageBlobBytes());
    }
  }

  return RestoreNextBlobFileNumber(subcompaction_progress);
}

// Attempt to resume compaction from a previously persisted compaction progress.
//
// RETURNS:
// - Status::OK():
// * Input iterator positioned at next unprocessed key
// * CompactionOutputs objects fully restored for both output and proximal
// output levels in SubcompactionState, including BlobDB additions and garbage
// * Compaction job statistics accurately reflect input and output records
// processed for record count verification
// * SST and blob file-number generation advanced to prevent conflicts with
// existing outputs
// - Status::NotFound(): No valid progress to resume from
// - Status::Incomplete(): Blob state cannot be resumed safely
// - Status::Corruption(): Resume key is invalid, beyond input range, or output
// restoration failed
Status CompactionJob::MaybeResumeSubcompactionProgressOnInputIterator(
    SubcompactionState* sub_compact, InternalIterator* input_iter) {
  const ReadOptions read_options(Env::IOActivity::kCompaction);
  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();
  SubcompactionProgress& subcompaction_progress =
      sub_compact->GetSubcompactionProgressRef();

  if (subcompaction_progress.output_level_progress
              .GetNumProcessedOutputRecords() == 0 &&
      subcompaction_progress.proximal_output_level_progress
              .GetNumProcessedOutputRecords() == 0) {
    return Status::NotFound("No subcompaction progress to resume");
  }

  ROCKS_LOG_INFO(db_options_.info_log, "[%s] [JOB %d] Resuming compaction : %s",
                 cfd->GetName().c_str(), job_id_,
                 subcompaction_progress.ToString().c_str());

  input_iter->Seek(subcompaction_progress.next_internal_key_to_compact);

  if (!input_iter->Valid()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "[%s] [JOB %d] Iterator is invalid after "
                    "seeking to the key to resume. This indicates the key is "
                    "incorrectly beyond the input data range.",
                    cfd->GetName().c_str(), job_id_);
    return Status::Corruption(
        "The key to resume is beyond the input data range");
  } else if (!input_iter->status().ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "[%s] [JOB %d] Iterator has error after seeking to "
                    "the key to resume: %s",
                    cfd->GetName().c_str(), job_id_,
                    input_iter->status().ToString().c_str());
    return Status::Corruption(
        "Iterator has error status after seeking to the key: " +
        input_iter->status().ToString());
  }

  sub_compact->compaction_job_stats.has_accurate_num_input_records =
      subcompaction_progress.num_processed_input_records != 0;

  sub_compact->compaction_job_stats.num_input_records =
      subcompaction_progress.num_processed_input_records;

  for (const bool& is_proximal_level : {false, true}) {
    if (is_proximal_level &&
        !sub_compact->compaction->SupportsPerKeyPlacement()) {
      continue;
    }

    Status s;
    SubcompactionProgressPerLevel& subcompaction_progress_per_level =
        is_proximal_level
            ? subcompaction_progress.proximal_output_level_progress
            : subcompaction_progress.output_level_progress;

    const auto& output_files =
        subcompaction_progress_per_level.GetOutputFiles();

    std::vector<std::shared_ptr<const TableProperties>>
        output_files_table_properties;

    // TODO(hx235): investigate if we can skip reading properties to save read
    // IO
    s = ReadOutputFilesTableProperties(output_files, read_options,
                                       output_files_table_properties);
    if (!s.ok()) {
      ROCKS_LOG_ERROR(
          db_options_.info_log,
          "[%s] [JOB %d] Failed to read table properties for %s output level"
          "files "
          "during resume: %s.",
          cfd->GetName().c_str(), job_id_, is_proximal_level ? "proximal" : "",
          s.ToString().c_str());
      return Status::Corruption(
          "Not able to resume due to table property reading error " +
          s.ToString());
    }

    RestoreCompactionOutputs(cfd, output_files_table_properties,
                             subcompaction_progress_per_level,
                             sub_compact->Outputs(is_proximal_level));

    // Skip past all the used file numbers to avoid creating new output files
    // after resumption that conflict with the existing output files
    for (const auto& file_meta : output_files) {
      uint64_t file_number = file_meta.fd.GetNumber();
      while (versions_->NewFileNumber() <= file_number) {
        versions_->FetchAddFileNumber(1);
      }
    }
  }

  Status blob_status =
      RestoreBlobCompactionProgress(sub_compact, subcompaction_progress);
  if (!blob_status.ok()) {
    return blob_status;
  }

  return Status::OK();
}

void CompactionJob::UpdateSubcompactionProgress(
    const CompactionIterator* c_iter, const Slice next_table_min_key,
    SubcompactionState* sub_compact) {
  assert(c_iter);
  SubcompactionProgress& subcompaction_progress =
      sub_compact->GetSubcompactionProgressRef();

  IterKey next_ikey_to_compact;
  next_ikey_to_compact.SetInternalKey(ExtractUserKey(next_table_min_key),
                                      kMaxSequenceNumber, kValueTypeForSeek);
  subcompaction_progress.next_internal_key_to_compact =
      next_ikey_to_compact.GetInternalKey().ToString();

  // Track total processed input records for progress reporting by combining:
  // - Resumed count: records already processed before compaction was
  // interrupted
  // - Current count: records scanned in the current compaction session
  // Only update when both tracking mechanisms provide accurate counts to ensure
  // reliability.
  subcompaction_progress.num_processed_input_records =
      c_iter->HasNumInputEntryScanned() &&
              sub_compact->compaction_job_stats.has_accurate_num_input_records
          ? c_iter->NumInputEntryScanned() +
                sub_compact->compaction_job_stats.num_input_records
          : 0;

  UpdateSubcompactionProgressPerLevel(
      sub_compact, false /* is_proximal_level */, subcompaction_progress);

  if (sub_compact->compaction->SupportsPerKeyPlacement()) {
    UpdateSubcompactionProgressPerLevel(
        sub_compact, true /* is_proximal_level */, subcompaction_progress);
  }

  UpdateBlobCompactionProgress(sub_compact, subcompaction_progress);
}

void CompactionJob::UpdateBlobCompactionProgress(
    SubcompactionState* sub_compact,
    SubcompactionProgress& subcompaction_progress) {
  CompactionOutputs* const outputs = sub_compact->Outputs(false);
  assert(outputs);

  const MutableCFOptions& mutable_cf_options =
      sub_compact->compaction->mutable_cf_options();
  const bool can_create_blob_files =
      mutable_cf_options.enable_blob_files &&
      sub_compact->compaction->output_level() >=
          mutable_cf_options.blob_file_starting_level;
  const BlobGarbageMeter* garbage_meter =
      sub_compact->GetBlobGarbageCheckpoint();
  if (garbage_meter == nullptr) {
    garbage_meter = outputs->GetBlobGarbageMeter();
  }
  if (!can_create_blob_files && garbage_meter == nullptr) {
    return;
  }

  subcompaction_progress.has_blob_compaction_progress = true;
  subcompaction_progress.blob_file_additions = outputs->GetBlobFileAdditions();
  subcompaction_progress.blob_file_garbages.clear();
  if (garbage_meter != nullptr) {
    for (const auto& [file_number, flow] : garbage_meter->flows()) {
      assert(flow.IsValid());
      if (flow.HasGarbage()) {
        subcompaction_progress.blob_file_garbages.emplace_back(
            file_number, flow.GetGarbageCount(), flow.GetGarbageBytes());
      }
    }
    std::sort(subcompaction_progress.blob_file_garbages.begin(),
              subcompaction_progress.blob_file_garbages.end(),
              [](const BlobFileGarbage& lhs, const BlobFileGarbage& rhs) {
                return lhs.GetBlobFileNumber() < rhs.GetBlobFileNumber();
              });
  }
  subcompaction_progress.next_blob_file_number = GetNextBlobFileNumber();
}

void CompactionJob::UpdateSubcompactionProgressPerLevel(
    SubcompactionState* sub_compact, bool is_proximal_level,
    SubcompactionProgress& subcompaction_progress) {
  SubcompactionProgressPerLevel& subcompaction_progress_per_level =
      is_proximal_level ? subcompaction_progress.proximal_output_level_progress
                        : subcompaction_progress.output_level_progress;

  subcompaction_progress_per_level.SetNumProcessedOutputRecords(
      sub_compact->OutputStats(is_proximal_level)->num_output_records);

  const auto& prev_output_files =
      subcompaction_progress_per_level.GetOutputFiles();

  const auto& current_output_files =
      sub_compact->Outputs(is_proximal_level)->GetOutputs();

  for (size_t i = prev_output_files.size(); i < current_output_files.size();
       i++) {
    subcompaction_progress_per_level.AddToOutputFiles(
        current_output_files[i].meta);
  }
}

Status CompactionJob::PersistSubcompactionProgress(
    SubcompactionState* sub_compact) {
  SubcompactionProgress& subcompaction_progress =
      sub_compact->GetSubcompactionProgressRef();

  assert(compaction_progress_writer_);

  VersionEdit edit;
  edit.SetSubcompactionProgress(subcompaction_progress);

  std::string record;
  if (!edit.EncodeTo(&record)) {
    ROCKS_LOG_ERROR(
        db_options_.info_log,
        "[%s] [JOB %d] Failed to encode subcompaction "
        "progress",
        compact_->compaction->column_family_data()->GetName().c_str(), job_id_);
    return Status::Corruption("Failed to encode subcompaction progress");
  }

  WriteOptions write_options(Env::IOActivity::kCompaction);
  Status s = compaction_progress_writer_->AddRecord(write_options, record);
  IOOptions opts;
  if (s.ok()) {
    s = WritableFileWriter::PrepareIOOptions(write_options, opts);
  }
  if (s.ok()) {
    s = db_options_.use_fsync ? compaction_progress_writer_->file()->Fsync(opts)
                              : compaction_progress_writer_->file()->Sync(opts);
  }

  if (!s.ok()) {
    ROCKS_LOG_ERROR(
        db_options_.info_log,
        "[%s] [JOB %d] Failed to persist subcompaction "
        "progress: %s",
        compact_->compaction->column_family_data()->GetName().c_str(), job_id_,
        s.ToString().c_str());
    return s;
  }

  TEST_SYNC_POINT("CompactionJob::PersistSubcompactionProgress:Complete");

  subcompaction_progress.output_level_progress
      .UpdateLastPersistedOutputFilesCount();

  subcompaction_progress.proximal_output_level_progress
      .UpdateLastPersistedOutputFilesCount();

  subcompaction_progress.UpdateLastPersistedBlobFileAdditionsCount();

  return Status::OK();
}

Status CompactionJob::VerifyInputRecordCount(
    uint64_t num_input_range_del) const {
  size_t ts_sz = compact_->compaction->column_family_data()
                     ->user_comparator()
                     ->timestamp_size();
  // When trim_ts_ is non-empty, CompactionIterator takes
  // HistoryTrimmingIterator as input iterator and sees a trimmed view of
  // input keys. So the number of keys it processed is not suitable for
  // verification here.
  // TODO: support verification when trim_ts_ is non-empty.
  if (!(ts_sz > 0 && !trim_ts_.empty())) {
    assert(internal_stats_.output_level_stats.num_input_records > 0);
    // TODO: verify the number of range deletion entries.
    uint64_t expected = internal_stats_.output_level_stats.num_input_records -
                        num_input_range_del;
    uint64_t actual = job_stats_->num_input_records;
    TEST_SYNC_POINT_CALLBACK("CompactionJob::VerifyInputRecordCount:Actual",
                             &actual);
    if (expected != actual) {
      char scratch[2345];
      compact_->compaction->Summary(scratch, sizeof(scratch));
      std::string msg =
          "Compaction number of input keys does not match "
          "number of keys processed. Expected " +
          std::to_string(expected) + " but processed " +
          std::to_string(actual) + ". Compaction summary: " + scratch;
      ROCKS_LOG_WARN(
          db_options_.info_log,
          "[%s] [JOB %d] VerifyInputRecordCount() Status: %s",
          compact_->compaction->column_family_data()->GetName().c_str(),
          job_context_->job_id, msg.c_str());
      if (db_options_.compaction_verify_record_count) {
        return Status::Corruption(msg);
      }
    }
  }
  return Status::OK();
}

Status CompactionJob::VerifyOutputRecordCount() const {
  uint64_t total_output_num = 0;
  for (const auto& state : compact_->sub_compact_states) {
    for (const auto& output : state.GetOutputs()) {
      total_output_num += output.table_properties->num_entries -
                          output.table_properties->num_range_deletions;
    }
  }

  uint64_t expected = internal_stats_.output_level_stats.num_output_records;
  if (internal_stats_.has_proximal_level_output) {
    expected += internal_stats_.proximal_level_stats.num_output_records;
  }
  if (expected != total_output_num) {
    char scratch[2345];
    compact_->compaction->Summary(scratch, sizeof(scratch));
    std::string msg =
        "Number of keys in compaction output SST files does not match "
        "number of keys added. Expected " +
        std::to_string(expected) + " but there are " +
        std::to_string(total_output_num) +
        " in output SST files. Compaction summary: " + scratch;
    ROCKS_LOG_WARN(
        db_options_.info_log,
        "[%s] [JOB %d] VerifyOutputRecordCount() status: %s",
        compact_->compaction->column_family_data()->GetName().c_str(),
        job_context_->job_id, msg.c_str());
    if (db_options_.compaction_verify_record_count) {
      return Status::Corruption(msg);
    }
  }
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
