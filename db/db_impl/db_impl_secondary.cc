//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_impl/db_impl_secondary.h"

#include <algorithm>
#include <cinttypes>
#include <optional>
#include <unordered_set>

#include "db/arena_wrapped_db_iter.h"
#include "db/blob/blob_fetcher.h"
#include "db/db_impl/db_impl_metadata.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/merge_context.h"
#include "db/version_edit.h"
#include "file/filename.h"
#include "file/writable_file_writer.h"
#include "logging/auto_roll_logger.h"
#include "logging/logging.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/options_util.h"
#include "util/atomic.h"
#include "util/cast_util.h"
#include "util/write_batch_util.h"

namespace ROCKSDB_NAMESPACE {

DBImplSecondary::DBImplSecondary(const DBOptions& db_options,
                                 const std::string& dbname,
                                 std::string secondary_path)
    : DBImpl(db_options, dbname, false, true, true),
      secondary_path_(std::move(secondary_path)) {
  secondary_read_view_reclaimer_.reset(new port::Thread(
      &DBImplSecondary::ReclaimRetiredSecondaryReadViewsLoop, this));
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "Opening the db in secondary mode");
  LogFlush(immutable_db_options_.info_log);
}

DBImplSecondary::~DBImplSecondary() {
  ResetSecondaryReadView();
  StopSecondaryReadViewReclaimer();
}

// REQUIRES: mutex_ not held. CleanupSuperVersion() locks mutex_ to release a
// SuperVersion's last reference, so dropping the last reference to a view while
// mutex_ is held self-deadlocks on the non-reentrant mutex.
DBImplSecondary::SecondaryReadView::~SecondaryReadView() {
  for (const SuperVersions::value_type& entry : super_versions) {
    db->CleanupSuperVersion(entry.second);
  }
}

void DBImplSecondary::PublishSecondaryReadView() {
  mutex_.AssertHeld();

  if (secondary_read_view_closed_) {
    // A catch-up that raced ResetSecondaryReadView() must not republish, or the
    // new view would outlive the drain that close waits on.
    return;
  }

  std::shared_ptr<SecondaryReadView> new_view =
      std::make_shared<SecondaryReadView>(this, versions_->LastSequence());
  for (ColumnFamilyData* cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->IsDropped() || !cfd->initialized()) {
      continue;
    }
    SuperVersion* super_version = cfd->GetSuperVersion();
    if (super_version == nullptr) {
      continue;
    }
    new_view->super_versions.emplace(cfd->GetID(), super_version->Ref());
  }

  // Load the outgoing view before the store, and keep that reference until it
  // reaches the retirement queue. The store drops the published reference, so
  // without this local it could be the last one and ~SecondaryReadView() would
  // run here, under mutex_. Retiring before the store would leave the view
  // reachable by readers after it was queued for reclamation.
  std::shared_ptr<const SecondaryReadView> previous_view =
      AtomicSharedPtrLoad(&secondary_read_view_, std::memory_order_acquire);
  AtomicSharedPtrStore(
      &secondary_read_view_,
      std::shared_ptr<const SecondaryReadView>(std::move(new_view)),
      std::memory_order_release);
  RetireSecondaryReadView(std::move(previous_view));
}

void DBImplSecondary::RetireSecondaryReadView(
    std::shared_ptr<const SecondaryReadView> read_view) {
  if (read_view == nullptr) {
    return;
  }
  {
    std::lock_guard<std::mutex> lock(retired_secondary_read_views_mutex_);
    retired_secondary_read_views_.emplace_back(std::move(read_view));
  }
  // Publish and close synchronously reclaim after retiring a view. Do not wake
  // the background reclaimer here: it could take a reader-free view first and
  // let catch-up scan for obsolete files before releasing the view's
  // SuperVersions. A reader that makes a retained view reclaimable later wakes
  // the reclaimer in ReleaseSecondaryReadView().
}

bool DBImplSecondary::HasReclaimableSecondaryReadView() const {
  for (const auto& read_view : retired_secondary_read_views_) {
    if (read_view.use_count() == 1) {
      return true;
    }
  }
  return false;
}

bool DBImplSecondary::AllSecondaryReadViewsReclaimed() const {
  return retired_secondary_read_views_.empty() &&
         secondary_read_view_reclaims_in_flight_ == 0;
}

bool DBImplSecondary::ReclaimRetiredSecondaryReadViews() {
  // Keep moved views alive until after the retirement lock is released. Their
  // destructors call CleanupSuperVersion(), which can lock mutex_.
  std::vector<std::shared_ptr<const SecondaryReadView>> reclaimable_views;
  size_t num_reclaiming;
  {
    std::lock_guard<std::mutex> lock(retired_secondary_read_views_mutex_);
    for (auto iter = retired_secondary_read_views_.begin();
         iter != retired_secondary_read_views_.end();) {
      // The view is no longer published, so its reference count can only
      // decrease. A count of one means the retirement queue is the sole owner.
      if (iter->use_count() == 1) {
        reclaimable_views.emplace_back(std::move(*iter));
        iter = retired_secondary_read_views_.erase(iter);
      } else {
        ++iter;
      }
    }
    num_reclaiming = reclaimable_views.size();
    secondary_read_view_reclaims_in_flight_ += num_reclaiming;
    if (num_reclaiming == 0) {
      return AllSecondaryReadViewsReclaimed();
    }
  }

  TEST_SYNC_POINT("DBImplSecondary::ReclaimRetiredReadViews:BeforeReclaim");
  reclaimable_views.clear();
  bool all_reclaimed;
  {
    std::lock_guard<std::mutex> lock(retired_secondary_read_views_mutex_);
    assert(secondary_read_view_reclaims_in_flight_ >= num_reclaiming);
    secondary_read_view_reclaims_in_flight_ -= num_reclaiming;
    all_reclaimed = AllSecondaryReadViewsReclaimed();
  }
  retired_secondary_read_views_cv_.notify_all();
  TEST_SYNC_POINT("DBImplSecondary::ReclaimRetiredReadViews:AfterReclaim");
  return all_reclaimed;
}

void DBImplSecondary::ReclaimRetiredSecondaryReadViewsLoop() {
  while (true) {
    {
      std::unique_lock<std::mutex> lock(retired_secondary_read_views_mutex_);
      retired_secondary_read_views_cv_.wait(lock, [this]() {
        if (secondary_read_view_reclaimer_stopping_ &&
            AllSecondaryReadViewsReclaimed()) {
          return true;
        }
        return HasReclaimableSecondaryReadView();
      });
      if (secondary_read_view_reclaimer_stopping_ &&
          AllSecondaryReadViewsReclaimed()) {
        return;
      }
    }
    TEST_SYNC_POINT(
        "DBImplSecondary::ReclaimRetiredReadViewsLoop:BeforeReclaim");
    ReclaimRetiredSecondaryReadViews();
  }
}

void DBImplSecondary::CleanupRetiredSecondaryReadViews() {
  ReclaimRetiredSecondaryReadViews();
  std::unique_lock<std::mutex> lock(retired_secondary_read_views_mutex_);
  retired_secondary_read_views_cv_.wait(
      lock, [this]() { return secondary_read_view_reclaims_in_flight_ == 0; });
}

void DBImplSecondary::DrainRetiredSecondaryReadViews() {
  while (!ReclaimRetiredSecondaryReadViews()) {
    {
      std::unique_lock<std::mutex> lock(retired_secondary_read_views_mutex_);
      TEST_SYNC_POINT("DBImplSecondary::DrainRetiredReadViews:Wait");
      retired_secondary_read_views_cv_.wait(lock, [this]() {
        if (AllSecondaryReadViewsReclaimed()) {
          return true;
        }
        return HasReclaimableSecondaryReadView();
      });
    }
    TEST_SYNC_POINT("DBImplSecondary::DrainRetiredReadViews:AfterWait");
  }
}

void DBImplSecondary::ReleaseSecondaryReadView(
    std::shared_ptr<const SecondaryReadView>* read_view) {
  read_view->reset();
  {
    // Synchronize the reference-count change with the wait predicate so its
    // notification cannot be lost.
    std::lock_guard<std::mutex> lock(retired_secondary_read_views_mutex_);
    retired_secondary_read_views_cv_.notify_all();
  }
}

void DBImplSecondary::StopSecondaryReadViewReclaimer() {
  if (secondary_read_view_reclaimer_ == nullptr) {
    return;
  }
  {
    std::lock_guard<std::mutex> lock(retired_secondary_read_views_mutex_);
    secondary_read_view_reclaimer_stopping_ = true;
  }
  retired_secondary_read_views_cv_.notify_all();
  TEST_SYNC_POINT("DBImplSecondary::StopSecondaryReadViewReclaimer:BeforeJoin");
  secondary_read_view_reclaimer_->join();
  secondary_read_view_reclaimer_.reset();
}

void DBImplSecondary::ResetSecondaryReadView() {
  std::shared_ptr<const SecondaryReadView> read_view;
  {
    // mutex_ makes this read-modify-write atomic against
    // PublishSecondaryReadView(), which holds it too. Without that, a catch-up
    // racing close can read the same outgoing view and retire it twice, leaving
    // two queue entries that each see use_count() == 2 so the drain below never
    // finishes.
    InstrumentedMutexLock lock_guard(&mutex_);
    secondary_read_view_closed_ = true;
    read_view =
        AtomicSharedPtrLoad(&secondary_read_view_, std::memory_order_acquire);
    AtomicSharedPtrStore(&secondary_read_view_,
                         std::shared_ptr<const SecondaryReadView>(),
                         std::memory_order_release);
  }
  RetireSecondaryReadView(std::move(read_view));
  DrainRetiredSecondaryReadViews();
}

Status DBImplSecondary::CloseImpl() {
  // The view's SuperVersions pin their column families, so release them before
  // DBImpl::CloseImpl() destroys the VersionSet.
  ResetSecondaryReadView();
  StopSecondaryReadViewReclaimer();
  return DBImpl::CloseImpl();
}

Status DBImplSecondary::Recover(
    const std::vector<ColumnFamilyDescriptor>& column_families,
    bool /*readonly*/, bool /*error_if_wal_file_exists*/,
    bool /*error_if_data_exists_in_wals*/, bool /*is_retry*/, uint64_t*,
    RecoveryContext* /*recovery_ctx*/, bool* /*can_retry*/) {
  mutex_.AssertHeld();

  Status s;
  s = static_cast<ReactiveVersionSet*>(versions_.get())
          ->Recover(column_families, &manifest_reader_, &manifest_reporter_,
                    &manifest_reader_status_);
  if (!s.ok()) {
    if (manifest_reader_status_) {
      manifest_reader_status_->PermitUncheckedError();
    }
    return s;
  }
  // Initial max_total_in_memory_state_ before recovery logs.
  max_total_in_memory_state_ = 0;
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    const auto& mutable_cf_options = cfd->GetLatestMutableCFOptions();
    max_total_in_memory_state_ += mutable_cf_options.write_buffer_size *
                                  mutable_cf_options.max_write_buffer_number;
  }
  default_cf_handle_ = new ColumnFamilyHandleImpl(
      versions_->GetColumnFamilySet()->GetDefault(), this, &mutex_);
  default_cf_internal_stats_ = default_cf_handle_->cfd()->internal_stats();

  // TODO: update options_file_number_ needed?

  return s;
}

// find new WAL and apply them in order to the secondary instance
Status DBImplSecondary::FindAndRecoverLogFiles(
    std::unordered_set<ColumnFamilyData*>* cfds_changed,
    JobContext* job_context) {
  assert(nullptr != cfds_changed);
  assert(nullptr != job_context);
  TEST_SYNC_POINT("DBImplSecondary::FindAndRecoverLogFiles:Begin");
  Status s;
  std::vector<uint64_t> logs;
  s = FindNewLogNumbers(&logs);
  if (s.ok()) {
    // Empty recovery rounds still prune resolved transactions and completed
    // prep tracking.
    SequenceNumber next_sequence(kMaxSequenceNumber);
    s = RecoverLogFiles(logs, &next_sequence, cfds_changed, job_context);
  }
  return s;
}

// List wal_dir and find all new WALs, return these log numbers
Status DBImplSecondary::FindNewLogNumbers(std::vector<uint64_t>* logs) {
  assert(logs != nullptr);
  std::vector<std::string> filenames;
  Status s;
  IOOptions io_opts;
  io_opts.do_not_recurse = true;
  s = immutable_db_options_.fs->GetChildren(immutable_db_options_.GetWalDir(),
                                            io_opts, &filenames,
                                            /*IODebugContext*=*/nullptr);
  if (s.IsNotFound()) {
    return Status::InvalidArgument("Failed to open wal_dir",
                                   immutable_db_options_.GetWalDir());
  } else if (!s.ok()) {
    return s;
  }

  // Drop readers only after MANIFEST replay says the WAL is obsolete. The
  // primary can create a higher-number empty WAL before it stops appending to a
  // lower-number current WAL, so seeing a higher-number WAL is not sufficient.
  const uint64_t min_log_number_to_keep = versions_->min_log_number_to_keep();
  for (auto iter = log_readers_.begin(); iter != log_readers_.end();) {
    if (iter->first < min_log_number_to_keep) {
      iter = log_readers_.erase(iter);
    } else {
      break;
    }
  }

  // If log_readers_ is non-empty, it means we have applied all logs with log
  // numbers smaller than the smallest retained log reader. Otherwise, manifest
  // recovery tells us which older logs can be skipped.
  uint64_t log_number_min = min_log_number_to_keep;
  if (!log_readers_.empty()) {
    log_number_min = log_readers_.begin()->first;
  }
  for (size_t i = 0; i < filenames.size(); i++) {
    uint64_t number;
    FileType type;
    if (ParseFileName(filenames[i], &number, &type) && type == kWalFile &&
        number >= log_number_min) {
      logs->push_back(number);
    }
  }
  // Recover logs in the order that they were generated
  if (!logs->empty()) {
    std::sort(logs->begin(), logs->end());
  }
  return s;
}

Status DBImplSecondary::MaybeInitLogReader(
    uint64_t log_number, log::FragmentBufferedReader** log_reader) {
  auto iter = log_readers_.find(log_number);
  // make sure the log file is still present
  if (iter == log_readers_.end() ||
      iter->second->reader_->GetLogNumber() != log_number) {
    // delete the obsolete log reader if log number mismatch
    if (iter != log_readers_.end()) {
      log_readers_.erase(iter);
    }
    // initialize log reader from log_number
    // TODO: min_log_number_to_keep_2pc check needed?
    // Open the log file
    std::string fname =
        LogFileName(immutable_db_options_.GetWalDir(), log_number);
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Recovering log #%" PRIu64 " mode %d", log_number,
                   static_cast<int>(immutable_db_options_.wal_recovery_mode));

    std::unique_ptr<SequentialFileReader> file_reader;
    {
      std::unique_ptr<FSSequentialFile> file;
      Status status = fs_->NewSequentialFile(
          fname, fs_->OptimizeForLogRead(file_options_), &file, nullptr);
      if (!status.ok()) {
        *log_reader = nullptr;
        return status;
      }
      file_reader.reset(new SequentialFileReader(
          std::move(file), fname, immutable_db_options_.log_readahead_size,
          io_tracer_));
    }

    // Create the log reader.
    LogReaderContainer* log_reader_container = new LogReaderContainer(
        env_, immutable_db_options_.info_log, std::move(fname),
        std::move(file_reader), log_number);
    log_readers_.insert(std::make_pair(
        log_number, std::unique_ptr<LogReaderContainer>(log_reader_container)));
  }
  iter = log_readers_.find(log_number);
  assert(iter != log_readers_.end());
  *log_reader = iter->second->reader_;
  return Status::OK();
}

// After manifest recovery, replay WALs and refresh log_readers_ if necessary
// REQUIRES: log_numbers are sorted in ascending order
Status DBImplSecondary::RecoverLogFiles(
    const std::vector<uint64_t>& log_numbers, SequenceNumber* next_sequence,
    std::unordered_set<ColumnFamilyData*>* cfds_changed,
    JobContext* job_context) {
  assert(nullptr != cfds_changed);
  assert(nullptr != job_context);
  mutex_.AssertHeld();
  Status status;
  for (auto log_number : log_numbers) {
    log::FragmentBufferedReader* reader = nullptr;
    status = MaybeInitLogReader(log_number, &reader);
    if (!status.ok()) {
      return status;
    }
    assert(reader != nullptr);
  }

  DeleteResolvedRecoveredTransactions();

  const UnorderedMap<uint32_t, size_t>& running_ts_sz =
      versions_->GetRunningColumnFamiliesTimestampSize();
  for (auto log_number : log_numbers) {
    auto it = log_readers_.find(log_number);
    assert(it != log_readers_.end());
    log::FragmentBufferedReader* reader = it->second->reader_;
    Status* wal_read_status = it->second->status_;
    assert(wal_read_status);
    // Manually update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(log_number);

    // Determine if we should tolerate incomplete records at the tail end of the
    // Read all the records and add to a memtable
    std::string scratch;
    Slice record;
    WriteBatch batch;
    // Hoisted alongside `scratch`, `record` and `batch`: the loop below runs
    // once per replayed WAL record, and clear() keeps the bucket array that a
    // set constructed there would allocate again for every record.
    std::unordered_set<uint32_t> selected_cf_ids;

    while (reader->ReadRecord(&record, &scratch,
                              immutable_db_options_.wal_recovery_mode) &&
           wal_read_status->ok() && status.ok()) {
      if (record.size() < WriteBatchInternal::kHeader) {
        reader->GetReporter()->Corruption(
            record.size(), Status::Corruption("log record too small"));
        continue;
      }
      status = WriteBatchInternal::SetContents(&batch, record);
      if (!status.ok()) {
        break;
      }
      const UnorderedMap<uint32_t, size_t>& record_ts_sz =
          reader->GetRecordedTimestampSize();
      status = HandleWriteBatchTimestampSizeDifference(
          &batch, running_ts_sz, record_ts_sz,
          TimestampSizeConsistencyMode::kVerifyConsistency, seq_per_batch_,
          batch_per_txn_);
      if (!status.ok()) {
        break;
      }
      SequenceNumber seq_of_batch = WriteBatchInternal::Sequence(&batch);
      std::vector<uint32_t> column_family_ids;
      status = CollectColumnFamilyIdsFromWriteBatch(batch, &column_family_ids);
      if (status.ok()) {
        for (const auto id : column_family_ids) {
          ColumnFamilyData* cfd =
              versions_->GetColumnFamilySet()->GetColumnFamily(id);
          if (cfd == nullptr) {
            continue;
          }
          cfds_changed->insert(cfd);
          // The return value is what the seal below compares against
          // `log_number`, so the call has to stay. The write also names a WAL
          // for column families whose inserts are skipped as already flushed
          // and never reach the set recorded after the insert.
          const std::optional<uint64_t> prev_log_number =
              RecordCurrentLog(id, log_number);
          const std::vector<FileMetaData*>& l0_files =
              cfd->current()->storage_info()->LevelFiles(0);
          SequenceNumber seq =
              l0_files.empty() ? 0 : l0_files.back()->fd.largest_seqno;
          // `l0_files` is ordered newest first, so `seq` is the largest
          // sequence number in the oldest L0 file. A write batch at or below it
          // must reside in an SST that a prior MANIFEST replay already added.
          if (seq_of_batch <= seq) {
            continue;
          }
          // If the active memtable contains records added by replaying an
          // earlier WAL, then we need to seal the memtable, add it to the
          // immutable memtable list and create a new active memtable, so that
          // the sealed memtable can be dropped once the primary has flushed
          // that earlier WAL.
          //
          // This is best effort: the sequence number check above can skip the
          // seal without skipping the insert that follows, leaving the active
          // memtable holding entries from more than one WAL.
          // MaybeSealFullyFlushedActiveMemtable() tolerates that because
          // `cf_id_to_current_log_[id]` is the newest of them.
          if (!cfd->mem()->IsEmpty() && prev_log_number != log_number) {
            SealActiveMemtable(cfd, log_number, seq_of_batch, job_context);
          }
        }
        bool has_valid_writes = false;
        selected_cf_ids.clear();
        status = WriteBatchInternal::InsertInto(
            &batch, column_family_memtables_.get(),
            nullptr /* flush_scheduler */, nullptr /* trim_history_scheduler*/,
            true, log_number, this, false /* concurrent_memtable_writes */,
            next_sequence, &has_valid_writes, seq_per_batch_, batch_per_txn_,
            &selected_cf_ids);
        // A 2PC commit marker names no column family yet replays its prepared
        // batch into the memtables, so the recorded WAL would lag the committed
        // data and the gate would drop it. Mark them changed too, or a
        // commit-replay-only round skips the super version install and the
        // retention warning.
        //
        // The seal loop above runs over named ids, so a commit marker takes no
        // seal decision: its data joins the memtable's current WAL generation,
        // which then stays until the primary flushes the commit's WAL.
        for (const uint32_t id : selected_cf_ids) {
          RecordCurrentLog(id, log_number);
          ColumnFamilyData* cfd =
              versions_->GetColumnFamilySet()->GetColumnFamily(id);
          if (cfd != nullptr) {
            cfds_changed->insert(cfd);
          }
        }
      }
      // If column family was not found, it might mean that the WAL write
      // batch references to the column family that was dropped after the
      // insert. We don't want to fail the whole write batch in that case --
      // we just ignore the update.
      // That's why we set ignore missing column families to true
      // passing null flush_scheduler will disable memtable flushing which is
      // needed for secondary instances
      if (status.ok()) {
        auto last_sequence = *next_sequence - 1;
        if ((*next_sequence != kMaxSequenceNumber) &&
            (versions_->LastSequence() <= last_sequence)) {
          versions_->SetLastAllocatedSequence(last_sequence);
          versions_->SetLastPublishedSequence(last_sequence);
          versions_->SetLastSequence(last_sequence);
        }
      } else {
        // We are treating this as a failure while reading since we read valid
        // blocks that do not form coherent data
        reader->GetReporter()->Corruption(record.size(), status);
      }
    }
    if (status.ok() && !wal_read_status->ok()) {
      status = *wal_read_status;
    }
    if (!status.ok()) {
      wal_read_status->PermitUncheckedError();
      return status;
    }
  }
  return status;
}

void DBImplSecondary::SealActiveMemtable(ColumnFamilyData* cfd,
                                         uint64_t next_log_number,
                                         SequenceNumber new_mem_earliest_seq,
                                         JobContext* job_context) {
  mutex_.AssertHeld();
  assert(cfd != nullptr);
  assert(job_context != nullptr);
  MemTable* new_mem = cfd->ConstructNewMemtable(
      cfd->GetLatestMutableCFOptions(), new_mem_earliest_seq);
  cfd->mem()->SetNextLogNumber(next_log_number);
  cfd->mem()->ConstructFragmentedRangeTombstones();
  cfd->imm()->Add(cfd->mem(), &job_context->memtables_to_free);
  new_mem->Ref();
  cfd->SetMemtable(new_mem);
}

std::optional<uint64_t> DBImplSecondary::RecordCurrentLog(uint32_t cf_id,
                                                          uint64_t log_number) {
  mutex_.AssertHeld();
  const auto [log_iter, inserted] =
      cf_id_to_current_log_.try_emplace(cf_id, log_number);
  if (inserted) {
    return std::nullopt;
  }
  const uint64_t prev_log_number = log_iter->second;
  if (log_number > prev_log_number) {
    log_iter->second = log_number;
  }
  return prev_log_number;
}

bool DBImplSecondary::MaybeSealFullyFlushedActiveMemtable(
    ColumnFamilyData* cfd, uint64_t installed_log_number,
    JobContext* job_context) {
  mutex_.AssertHeld();
  assert(cfd != nullptr);
  assert(job_context != nullptr);
  assert(installed_log_number <= cfd->GetLogNumber());
  if (cfd->mem()->IsEmpty()) {
    return false;
  }
  const auto log_iter = cf_id_to_current_log_.find(cfd->GetID());
  if (log_iter == cf_id_to_current_log_.end()) {
    return false;
  }
  // Keep the memtable unless the installed Version covers everything it holds:
  // only the WALs older than `installed_log_number` are readable from the files
  // that Version references. The loop in TryCatchUpWithPrimary() explains why
  // the watermark has to come from the Version.
  if (log_iter->second >= installed_log_number) {
    return false;
  }
  // The file set is now the authority for these entries, so keeping them would
  // be incorrect rather than merely redundant: point lookups stop at the
  // memtable, and bottommost compaction rewriting the flushed entry's sequence
  // number to 0 makes the shadowing permanent.
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "[%s] Sealing active memtable replayed from WAL %" PRIu64
                 ", flushed by the primary and readable as of log number "
                 "%" PRIu64 "; its first sequence number is %" PRIu64,
                 cfd->GetName().c_str(), log_iter->second, installed_log_number,
                 static_cast<uint64_t>(cfd->mem()->GetFirstSequenceNumber()));
  // The memtable holds no entries from a WAL newer than `log_iter->second`, so
  // + 1 is the log number following its contents, and the check above puts that
  // at or below `installed_log_number`. The older immutable memtables that
  // RemoveOldMemTables() examines first were themselves sealed on a WAL switch
  // and stamped no higher, so nothing stops its scan short of this memtable and
  // it is collected in the same round.
  //
  // The replacement memtable's earliest sequence number is the VersionSet's
  // last sequence number, as in DBImplFollower::TryCatchUpWithLeader():
  // DBImpl::MultiCFSnapshot() compares it against the last sequence number to
  // detect a version change while collecting super version references without
  // the mutex. That can be above the sequence number replayed next, so it is
  // not yet the lower bound MemTable::SetEarliestSequenceNumber() documents;
  // MemTable::Add() corrects it downward on the first insert, and until then
  // the memtable holds no key a too-high bound could hide.
  SealActiveMemtable(cfd, log_iter->second + 1, versions_->LastSequence(),
                     job_context);
  cf_id_to_current_log_.erase(log_iter);
  return true;
}

void DBImplSecondary::MaybeWarnAboutRetainedMemtables(
    ColumnFamilyData* cfd, uint64_t installed_log_number) {
  mutex_.AssertHeld();
  assert(cfd != nullptr);
  // Tested before the count below because it is false whenever the secondary
  // is keeping up with the primary, which is the normal case.
  if (installed_log_number >= cfd->GetLogNumber()) {
    cf_id_to_retention_warning_.erase(cfd->GetID());
    return;
  }
  const int retained = cfd->imm()->NumNotFlushed();
  if (retained == 0) {
    cf_id_to_retention_warning_.erase(cfd->GetID());
    return;
  }
  // Warn only when either number changes. The condition lasts until the
  // primary's flushed files become readable, and TryCatchUpWithPrimary() is
  // called as often as the application chooses, so reporting it every round
  // would bury everything else in the log. Both values move only as the
  // condition worsens, making this one line per newly retained memtable.
  const std::pair<uint64_t, int> warning{installed_log_number, retained};
  const auto [warning_iter, inserted] =
      cf_id_to_retention_warning_.insert({cfd->GetID(), warning});
  if (!inserted) {
    if (warning_iter->second == warning) {
      return;
    }
    warning_iter->second = warning;
  }
  // ColumnFamilyData::RecalculateWriteStallConditions() also counts this as a
  // memtable limit stop, but attributes it to a flush that is not coming and to
  // a `max_write_buffer_number` that no writer of this instance's own can hit,
  // so name the actual cause.
  ROCKS_LOG_WARN(immutable_db_options_.info_log,
                 "[%s] Retaining %d immutable memtable(s): the primary has "
                 "flushed up to log number %" PRIu64
                 " but no Version past log number %" PRIu64
                 " could be installed, so they may hold the only readable copy "
                 "of what was flushed",
                 cfd->GetName().c_str(), retained, cfd->GetLogNumber(),
                 installed_log_number);
}

// The primary keeps min_log_number_to_keep at or below every unresolved prepare
// and every prepare with unflushed committed data. A recovered transaction
// entirely below it is therefore committed and flushed, or rolled back; see
// PrecomputeMinLogNumberToKeep2PC(). FindNewLogNumbers() uses the same
// threshold, so no marker below it can be replayed. If a resolution marker
// appears in a later WAL, its handler tolerates missing recovery state.
// Committed writes are skipped because the column family's log number is
// already past their WAL.
//
// Drop before replay so a reused name starts a new generation instead of
// merging with stale state. Open every reader first so a round that cannot
// replay drops nothing.
void DBImplSecondary::DeleteResolvedRecoveredTransactions() {
  mutex_.AssertHeld();
  if (recovered_transactions_.empty()) {
    return;
  }
  const uint64_t min_log_number_to_keep = versions_->min_log_number_to_keep();
  size_t deleted = 0;
  for (RecoveredTransactionMap::iterator it = recovered_transactions_.begin();
       it != recovered_transactions_.end();) {
    assert(!it->second->batches_.empty());
    bool all_batches_below_watermark = true;
    for (const auto& batch_info : it->second->batches_) {
      if (batch_info.second.log_number_ >= min_log_number_to_keep) {
        all_batches_below_watermark = false;
        break;
      }
    }
    if (all_batches_below_watermark) {
      it = DeleteRecoveredTransaction(it);
      ++deleted;
    } else {
      ++it;
    }
  }
  if (deleted > 0) {
    ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                    "Dropped %" ROCKSDB_PRIszt
                    " recovered transaction(s) prepared before WAL %" PRIu64,
                    deleted, min_log_number_to_keep);
  }
}

void DBImplSecondary::MultiGetWithMetadata(
    const ReadOptions& options, const size_t num_keys,
    ColumnFamilyHandle* const* column_families, const Slice* keys,
    PinnableSlice* values, Status* statuses,
    MultiGetOutputMetadata* output_metadata, const bool sorted_input) {
  // The only secondary-specific policy here is rejecting newer-version
  // tracking for explicit snapshots: catch-up can advance a secondary's view.
  // The remaining code initializes requested metadata and forwards to the
  // existing MultiGet implementation.
  std::vector<std::string>* timestamps = GetOutputTimestamps(output_metadata);
  if (timestamps != nullptr) {
    timestamps->resize(num_keys);
  }
  std::vector<uint8_t>* newer_version_present =
      GetOutputNewerVersionPresent(output_metadata);
  if (newer_version_present != nullptr) {
    newer_version_present->assign(num_keys, false);
    if (options.snapshot != nullptr) {
      const Status s = Status::NotSupported(
          "MultiGetWithMetadata is not supported in secondary DB mode");
      for (size_t i = 0; i < num_keys; ++i) {
        statuses[i] = s;
      }
      return;
    }
  }
  autovector<ColumnFamilyHandle*, MultiGetContext::MAX_BATCH_SIZE>
      stack_column_families;
  std::vector<ColumnFamilyHandle*> heap_column_families;
  ColumnFamilyHandle** mutable_column_families = MakeMutableCfHandles(
      column_families, num_keys, &stack_column_families, &heap_column_families);
  DBImpl::MultiGet(options, num_keys, mutable_column_families, keys, values,
                   timestamps != nullptr ? timestamps->data() : nullptr,
                   statuses, sorted_input);
}

void DBImplSecondary::MultiGetEntityWithMetadata(
    const ReadOptions& options, size_t num_keys,
    ColumnFamilyHandle* const* column_families, const Slice* keys,
    PinnableWideColumns* results, Status* statuses,
    MultiGetOutputMetadata* output_metadata, bool sorted_input) {
  Status s = InitMultiGetEntityOutputMetadata(num_keys, output_metadata);
  if (s.ok() && GetOutputNewerVersionPresent(output_metadata) != nullptr &&
      options.snapshot != nullptr) {
    // Catch-up can advance a secondary's view.
    s = Status::NotSupported(
        "MultiGetEntityWithMetadata is not supported in secondary DB mode");
  }
  if (!s.ok()) {
    std::fill_n(statuses, num_keys, s);
    return;
  }
  MultiGetEntityWithoutNewerVersionTracking(this, options, num_keys,
                                            column_families, keys, results,
                                            statuses, sorted_input);
}

void DBImplSecondary::MultiGetEntityWithMetadata(
    const ReadOptions& options, size_t num_keys, const Slice* keys,
    PinnableAttributeGroups* results, MultiGetOutputMetadata* output_metadata) {
  const size_t total_count = CountAttributeGroups(num_keys, results);
  Status s = InitMultiGetEntityOutputMetadata(total_count, output_metadata);
  if (!results) {
    s.PermitUncheckedError();
    return;
  }
  if (s.ok() && GetOutputNewerVersionPresent(output_metadata) != nullptr &&
      options.snapshot != nullptr) {
    s = Status::NotSupported(
        "MultiGetEntityWithMetadata is not supported in secondary DB mode");
  }
  if (!s.ok()) {
    SetAttributeGroupsStatus(num_keys, results, s);
    return;
  }
  MultiGetEntity(options, num_keys, keys, results);
}

Iterator* DBImplSecondary::NewIterator(const ReadOptions& _read_options,
                                       ColumnFamilyHandle* column_family) {
  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kDBIterator) {
    return NewErrorIterator(Status::InvalidArgument(
        "Can only call NewIterator with `ReadOptions::io_activity` is "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kDBIterator`"));
  }
  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kDBIterator;
  }
  if (read_options.read_tier == kPersistedTier) {
    return NewErrorIterator(Status::NotSupported(
        "ReadTier::kPersistedData is not yet supported in iterators."));
  }

  assert(column_family);
  if (read_options.timestamp) {
    const Status s =
        FailIfTsMismatchCf(column_family, *(read_options.timestamp));
    if (!s.ok()) {
      return NewErrorIterator(s);
    }
  } else {
    const Status s = FailIfCfHasTs(column_family);
    if (!s.ok()) {
      return NewErrorIterator(s);
    }
  }

  Iterator* result = nullptr;
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  assert(cfh != nullptr);
  auto cfd = cfh->cfd();
  if (read_options.tailing) {
    return NewErrorIterator(Status::NotSupported(
        "tailing iterator not supported in secondary mode"));
  } else if (read_options.snapshot != nullptr) {
    // TODO (yanqin) support snapshot.
    return NewErrorIterator(
        Status::NotSupported("snapshot not supported in secondary mode"));
  } else {
    SequenceNumber snapshot(kMaxSequenceNumber);
    SuperVersion* sv = cfd->GetReferencedSuperVersion(this);
    if (read_options.timestamp && read_options.timestamp->size() > 0) {
      const Status s =
          FailIfReadCollapsedHistory(cfd, sv, *(read_options.timestamp));
      if (!s.ok()) {
        CleanupSuperVersion(sv);
        return NewErrorIterator(s);
      }
    }
    result = NewIteratorImpl(read_options, cfh, sv, snapshot,
                             nullptr /*read_callback*/);
  }
  return result;
}

ArenaWrappedDBIter* DBImplSecondary::NewIteratorImpl(
    const ReadOptions& read_options, ColumnFamilyHandleImpl* cfh,
    SuperVersion* super_version, SequenceNumber snapshot,
    ReadCallback* read_callback, bool expose_blob_index, bool allow_refresh) {
  assert(nullptr != cfh);
  if (snapshot == kMaxSequenceNumber) {
    snapshot = versions_->LastSequence();
  }
  assert(snapshot != kMaxSequenceNumber);
  return NewArenaWrappedDbIterator(env_, read_options, cfh, super_version,
                                   snapshot, read_callback, this,
                                   expose_blob_index, allow_refresh,
                                   /*allow_mark_memtable_for_flush=*/false);
}

Status DBImplSecondary::NewIterators(
    const ReadOptions& _read_options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  if (column_families.empty()) {
    if (_read_options.io_activity != Env::IOActivity::kUnknown &&
        _read_options.io_activity != Env::IOActivity::kDBIterator) {
      return Status::InvalidArgument(
          "Can only call NewIterators with `ReadOptions::io_activity` is "
          "`Env::IOActivity::kUnknown` or `Env::IOActivity::kDBIterator`");
    }
    if (_read_options.read_tier == kPersistedTier) {
      return Status::NotSupported(
          "ReadTier::kPersistedData is not yet supported in iterators.");
    }
    if (_read_options.tailing) {
      return Status::NotSupported(
          "tailing iterator not supported in secondary mode");
    }
    if (_read_options.snapshot != nullptr) {
      return Status::NotSupported("snapshot not supported in secondary mode");
    }
  }
  return NewIteratorsImpl(
      std::vector<ReadOptions>(column_families.size(), _read_options),
      column_families, iterators);
}

Status DBImplSecondary::NewIterators(
    const std::vector<ReadOptions>& read_options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  return NewIteratorsImpl(read_options, column_families, iterators);
}

const Snapshot* DBImplSecondary::GetSnapshot() {
  TEST_SYNC_POINT("DBImplSecondary::GetSnapshot:BeforeMutexLock");
  return GetSnapshotImpl(/*is_write_conflict_boundary=*/false,
                         SnapshotLockMode::kAcquireDBMutex);
}

Status DBImplSecondary::NewIteratorsImpl(
    const std::vector<ReadOptions>& read_options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  if (read_options.size() != column_families.size()) {
    return Status::InvalidArgument(
        "read_options and column_families must have the same size");
  }
  if (iterators == nullptr) {
    return Status::InvalidArgument("iterators not allowed to be nullptr");
  }

  if (column_families.empty()) {
    iterators->clear();
    return Status::OK();
  }

  const Snapshot* const snapshot = read_options.front().snapshot;
  const bool tailing = read_options.front().tailing;
  std::vector<ReadOptions> normalized_read_options;
  normalized_read_options.reserve(read_options.size());
  for (size_t i = 0; i < read_options.size(); ++i) {
    const ReadOptions& options = read_options[i];
    if (options.io_activity != Env::IOActivity::kUnknown &&
        options.io_activity != Env::IOActivity::kDBIterator) {
      return Status::InvalidArgument(
          "Can only call NewIterators with `ReadOptions::io_activity` is "
          "`Env::IOActivity::kUnknown` or `Env::IOActivity::kDBIterator`");
    }
    if (options.read_tier == kPersistedTier) {
      return Status::NotSupported(
          "ReadTier::kPersistedData is not yet supported in iterators.");
    }
    if (options.snapshot != snapshot) {
      return Status::InvalidArgument(
          "All ReadOptions must use the same snapshot");
    }
    if (options.tailing != tailing) {
      return Status::InvalidArgument(
          "All ReadOptions must use the same tailing setting");
    }
    if (options.tailing) {
      return Status::NotSupported(
          "tailing iterator not supported in secondary mode");
    }
    if (options.snapshot != nullptr) {
      return Status::NotSupported("snapshot not supported in secondary mode");
    }

    auto* cf = column_families[i];
    assert(cf);
    Status s;
    if (options.timestamp) {
      s = FailIfTsMismatchCf(cf, *(options.timestamp));
    } else {
      s = FailIfCfHasTs(cf);
    }
    if (!s.ok()) {
      return s;
    }

    normalized_read_options.emplace_back(options);
    if (normalized_read_options.back().io_activity ==
        Env::IOActivity::kUnknown) {
      normalized_read_options.back().io_activity = Env::IOActivity::kDBIterator;
    }
  }

  iterators->clear();
  iterators->reserve(column_families.size());

  std::shared_ptr<const SecondaryReadView> read_view =
      AtomicSharedPtrLoad(&secondary_read_view_, std::memory_order_acquire);
  if (read_view == nullptr) {
    return Status::InvalidArgument("Secondary read view is not initialized");
  }
  TEST_SYNC_POINT("DBImplSecondary::NewIterators:AfterLoadReadView");

  const SequenceNumber read_seq = read_view->sequence;
  autovector<ColumnFamilySuperVersionPair, MultiGetContext::MAX_BATCH_SIZE>
      cf_sv_pairs;
  const auto cleanup_super_versions = [&]() {
    for (const auto& cf_sv_pair : cf_sv_pairs) {
      CleanupSuperVersion(cf_sv_pair.super_version);
    }
  };
  for (ColumnFamilyHandle* cf : column_families) {
    ColumnFamilyHandleImpl* cfh =
        static_cast_with_check<ColumnFamilyHandleImpl>(cf);
    const SecondaryReadView::SuperVersions::const_iterator sv_iter =
        read_view->super_versions.find(cfh->GetID());
    if (sv_iter == read_view->super_versions.end()) {
      if (!cfh->cfd()->IsDropped()) {
        cleanup_super_versions();
        ReleaseSecondaryReadView(&read_view);
        return Status::InvalidArgument(
            "Column family is not part of the secondary read view");
      }
      // A retained handle remains readable after its column family is dropped.
      // Its immutable final SuperVersion is intentionally not published, since
      // doing so would keep the dropped column family alive indefinitely.
      cf_sv_pairs.emplace_back(cfh,
                               cfh->cfd()->GetReferencedSuperVersion(this));
    } else {
      cf_sv_pairs.emplace_back(cfh, sv_iter->second->Ref());
    }
  }
  ReleaseSecondaryReadView(&read_view);
  TEST_SYNC_POINT("DBImplSecondary::NewIterators:AfterReleaseReadView");
  for (size_t i = 0; i < cf_sv_pairs.size(); ++i) {
    const auto& cf_sv_pair = cf_sv_pairs[i];
    const ReadOptions& options = normalized_read_options[i];
    if (options.timestamp && !options.timestamp->empty()) {
      const Status s = FailIfReadCollapsedHistory(
          cf_sv_pair.cfd, cf_sv_pair.super_version, *(options.timestamp));
      if (!s.ok()) {
        cleanup_super_versions();
        return s;
      }
    }
  }
  for (size_t i = 0; i < cf_sv_pairs.size(); ++i) {
    const auto& cf_sv_pair = cf_sv_pairs[i];
    iterators->push_back(NewIteratorImpl(
        normalized_read_options[i], cf_sv_pair.cfh, cf_sv_pair.super_version,
        read_seq, nullptr /* read_callback */, false /* expose_blob_index */,
        false /* allow_refresh */));
    TEST_SYNC_POINT("DBImplSecondary::NewIterators:AfterCreateIterator");
  }
  return Status::OK();
}

Status DBImplSecondary::TryCatchUpWithPrimary() {
  assert(versions_.get() != nullptr);
  Status s;
  // read the manifest and apply new changes to the secondary instance
  std::unordered_set<ColumnFamilyData*> cfds_changed;
  JobContext job_context(0, true /*create_superversion*/);
  {
    InstrumentedMutexLock lock_guard(&mutex_);
    assert(manifest_reader_.get() != nullptr);
    auto* reactive_versions =
        static_cast_with_check<ReactiveVersionSet>(versions_.get());
    s = reactive_versions->ReadAndApply(&mutex_, &manifest_reader_,
                                        manifest_reader_status_.get(),
                                        &cfds_changed,
                                        /*files_to_delete=*/nullptr);

    ROCKS_LOG_INFO(immutable_db_options_.info_log, "Last sequence is %" PRIu64,
                   static_cast<uint64_t>(versions_->LastSequence()));
    for (ColumnFamilyData* cfd : cfds_changed) {
      if (cfd->IsDropped()) {
        ROCKS_LOG_DEBUG(immutable_db_options_.info_log, "[%s] is dropped\n",
                        cfd->GetName().c_str());
        continue;
      }
      VersionStorageInfo::LevelSummaryStorage tmp;
      ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                      "[%s] Level summary: %s\n", cfd->GetName().c_str(),
                      cfd->current()->storage_info()->LevelSummary(&tmp));
    }

    // list wal_dir to discover new WALs and apply new changes to the secondary
    // instance
    bool replayed_every_wal_found = false;
    if (s.ok()) {
      s = FindAndRecoverLogFiles(&cfds_changed, &job_context);
      replayed_every_wal_found = s.ok();
      if (s.IsPathNotFound()) {
        ROCKS_LOG_INFO(
            immutable_db_options_.info_log,
            "Secondary tries to read WAL, but WAL file(s) have already "
            "been purged by primary.");
        s = Status::OK();
      }
    }
    if (s.ok()) {
      // Iterate over every column family rather than only `cfds_changed`: a
      // stale memtable left behind because an earlier round failed after
      // ReadAndApply() had already advanced the log number must still be
      // dropped, and that column family may have no new MANIFEST record in this
      // round. Column families dropped in this round are skipped, unlike with
      // `cfds_changed`, which can still name a column family that a later
      // record in the same round made VersionEditHandler::DestroyCfAndCleanup()
      // drop. Column families that neither changed nor have anything to collect
      // are skipped below, so this installs no extra super versions.
      for (ColumnFamilyData* cfd : *versions_->GetColumnFamilySet()) {
        if (cfd->IsDropped()) {
          // Nothing more can ever be replayed into a dropped column family, so
          // drop its entry too: it had no removal path before. Ids are never
          // reused, so a leftover entry could only waste space, and one is
          // still left behind by a column family destroyed before this loop
          // next runs.
          cf_id_to_current_log_.erase(cfd->GetID());
          cf_id_to_retention_warning_.erase(cfd->GetID());
          continue;
        }
        if (!cfd->initialized()) {
          // There is no memtable to reconcile yet, and any entry the column
          // family already has still names the WAL a later round must gate on.
          continue;
        }
        // Drop memtables against the log number of the Version most recently
        // installed, never `cfd->GetLogNumber()`: that advances as soon as the
        // primary's flush record is read, even when the flushed files cannot be
        // opened or an incomplete atomic group parks the new Version. Until
        // such a Version is installed the memtable can hold the only readable
        // copy of what was flushed, so dropping it against that number would
        // turn a stale read into a vanished key.
        const uint64_t installed_log_number =
            reactive_versions->GetInstalledVersionLogNumber(cfd->GetID());
        const bool sealed = MaybeSealFullyFlushedActiveMemtable(
            cfd, installed_log_number, &job_context);
        // A round that failed after RecoverLogFiles() had sealed on a WAL
        // switch can also leave a collectible immutable memtable behind, so
        // check for one instead of relying on this round having sealed.
        const bool needs_super_version =
            sealed ||
            cfd->imm()->HasOldMemTablesToRemove(installed_log_number) ||
            cfds_changed.count(cfd) > 0;
        if (!needs_super_version) {
          continue;
        }
        cfd->imm()->RemoveOldMemTables(installed_log_number,
                                       &job_context.memtables_to_free);
        MaybeWarnAboutRetainedMemtables(cfd, installed_log_number);
        auto& sv_context = job_context.superversion_contexts.back();
        cfd->InstallSuperVersion(&sv_context, &mutex_);
        sv_context.NewSuperVersion();
      }
      if (replayed_every_wal_found) {
        // Secondary catch-up does not run the primary paths that prune the
        // completed tracker prefix. Replay and pre-replay cleanup add
        // completions; an outstanding prepare stops the scan.
        (void)logs_with_prep_tracker_.FindMinLogContainingOutstandingPrep();
      }
      TEST_SYNC_POINT(
          "DBImplSecondary::TryCatchUpWithPrimary:BeforePublishReadView");
      TEST_SYNC_POINT(
          "DBImplSecondary::TryCatchUpWithPrimary:AllowPublishReadView");
      PublishSecondaryReadView();
    }
  }
  CleanupRetiredSecondaryReadViews();
  job_context.Clean();

  // Cleanup unused, obsolete files.
  JobContext purge_files_job_context(0);
  {
    InstrumentedMutexLock lock_guard(&mutex_);
    // Currently, secondary instance does not own the database files, thus it
    // is unnecessary for the secondary to force full scan.
    FindObsoleteFiles(&purge_files_job_context, /*force=*/false);
  }
  if (purge_files_job_context.HaveSomethingToDelete()) {
    PurgeObsoleteFiles(purge_files_job_context);
  }
  purge_files_job_context.Clean();
  return s;
}

Status DB::OpenAsSecondary(const Options& options, const std::string& dbname,
                           const std::string& secondary_path,
                           std::unique_ptr<DB>* dbptr) {
  *dbptr = nullptr;

  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName, cf_options);
  std::vector<ColumnFamilyHandle*> handles;

  Status s = DB::OpenAsSecondary(db_options, dbname, secondary_path,
                                 column_families, &handles, dbptr);
  if (s.ok()) {
    assert(handles.size() == 1);
    delete handles[0];
  }
  return s;
}

Status DB::OpenAsSecondary(
    const DBOptions& db_options, const std::string& dbname,
    const std::string& secondary_path,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, std::unique_ptr<DB>* dbptr) {
  return DBImplSecondary::OpenAsSecondaryImpl(
      db_options, dbname, secondary_path, column_families, handles, dbptr,
      /*recover_wal=*/true, /*trust_manifest_recovery=*/false);
}

Status DBImplSecondary::OpenAsSecondaryImpl(
    const DBOptions& db_options, const std::string& dbname,
    const std::string& secondary_path,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, std::unique_ptr<DB>* dbptr,
    bool recover_wal, bool trust_manifest_recovery) {
  *dbptr = nullptr;
  // Only normal DB::OpenAsSecondary recovery can defer SST opening.
  // `recover_wal` excludes OpenAndCompact even when no snapshot floor is
  // supplied, `open_files_async` is the explicit opt-in, and trust-MANIFEST
  // recovery must only open its protected compaction inputs.
  const bool defer_sst_file_opening =
      recover_wal && db_options.open_files_async && !trust_manifest_recovery;
  if (defer_sst_file_opening && !db_options.skip_stats_update_on_db_open) {
    return Status::InvalidArgument(
        "open_files_async requires skip_stats_update_on_db_open = true.");
  }
  if (defer_sst_file_opening) {
    for (const ColumnFamilyDescriptor& column_family : column_families) {
      if (column_family.options.compaction_style == kCompactionStyleFIFO) {
        return Status::NotSupported(
            "FIFO compaction is not supported with open_files_async = true.");
      }
    }
  }

  DBOptions tmp_opts(db_options);
  Status s;
  if (nullptr == tmp_opts.info_log) {
    s = CreateLoggerFromOptions(secondary_path, tmp_opts, &tmp_opts.info_log);
    if (!s.ok()) {
      tmp_opts.info_log = nullptr;
      return s;
    }
  }

  assert(tmp_opts.info_log != nullptr);
  if (db_options.max_open_files != -1 || defer_sst_file_opening) {
    std::ostringstream oss;
    oss << "The primary instance may delete all types of files after they "
           "become obsolete. The application can coordinate the primary and "
           "secondary so that primary does not delete/rename files that are "
           "currently being used by the secondary. Alternatively, a custom "
           "Env/FS can be provided such that files become inaccessible only "
           "after all primary and secondaries indicate that they are obsolete "
           "and deleted. ";
    if (defer_sst_file_opening) {
      oss << "With `open_files_async=true`, `max_open_files==-1` does not pin "
             "all table files before OpenAsSecondary returns. ";
    } else {
      oss << "If the above two are not possible, you can open the secondary "
             "instance with `max_open_files==-1` so that secondary will "
             "eagerly keep all table files open. Even if a file is deleted, "
             "its content can still be accessed via a prior open file "
             "descriptor. This is a hacky workaround for only table files. ";
    }
    oss << "If none of the above is done, then point lookup or "
           "range scan via the secondary instance can result in IOError: file "
           "not found. TryCatchUpWithPrimary() can resolve a transient, "
           "non-latched error if a newer primary version no longer references "
           "the missing file. A latched asynchronous-open background error "
           "requires closing and reopening the secondary after fixing the "
           "underlying problem.";
    ROCKS_LOG_WARN(tmp_opts.info_log, "%s", oss.str().c_str());
  }

  handles->clear();
  DBImplSecondary* impl = new DBImplSecondary(tmp_opts, dbname, secondary_path);
  impl->versions_.reset(new ReactiveVersionSet(
      dbname, &impl->immutable_db_options_, impl->mutable_db_options_,
      impl->file_options_, impl->table_cache_.get(),
      impl->write_buffer_manager_, &impl->write_controller_, impl->io_tracer_,
      impl->db_id_, impl->db_session_id_, defer_sst_file_opening));
  ReactiveVersionSet* reactive_versions =
      static_cast_with_check<ReactiveVersionSet>(impl->versions_.get());
  reactive_versions->SetTrustManifestRecovery(trust_manifest_recovery);
  impl->column_family_memtables_.reset(
      new ColumnFamilyMemTablesImpl(impl->versions_->GetColumnFamilySet()));
  impl->wal_in_db_path_ = impl->immutable_db_options_.IsWalDirSameAsDBPath();

  impl->mutex_.Lock();
  JobContext job_context(0);
  s = impl->Recover(column_families, true, false, false);
  // WAL recovery is optional: DB::OpenAsSecondary() needs it to replay
  // memtable data, while DB::OpenAndCompact() skips it since remote
  // compaction only needs LSM state from MANIFEST.
  if (s.ok() && recover_wal) {
    std::unordered_set<ColumnFamilyData*> cfds_changed;
    s = impl->FindAndRecoverLogFiles(&cfds_changed, &job_context);
    if (s.IsPathNotFound()) {
      ROCKS_LOG_INFO(impl->immutable_db_options_.info_log,
                     "Secondary tries to read WAL, but WAL file(s) have "
                     "already been purged by primary.");
      s = Status::OK();
    }
  }
  if (s.ok()) {
    for (const auto& cf : column_families) {
      auto cfd =
          impl->versions_->GetColumnFamilySet()->GetColumnFamily(cf.name);
      if (nullptr == cfd) {
        s = Status::InvalidArgument("Column family not found", cf.name);
        break;
      }
      handles->push_back(new ColumnFamilyHandleImpl(cfd, impl, &impl->mutex_));
    }
  }
  SuperVersionContext sv_context(true /* create_superversion */);
  if (s.ok()) {
    for (auto cfd : *impl->versions_->GetColumnFamilySet()) {
      sv_context.NewSuperVersion();
      cfd->InstallSuperVersion(&sv_context, &impl->mutex_);
    }
    impl->PublishSecondaryReadView();
    if (defer_sst_file_opening) {
      impl->ScheduleAsyncFileOpening();
    } else {
      impl->MarkAsyncFileOpenNotNeeded();
    }
  }
  impl->mutex_.Unlock();
  impl->CleanupRetiredSecondaryReadViews();
  sv_context.Clean();
  job_context.Clean();
  if (s.ok()) {
    dbptr->reset(impl);
    for (auto h : *handles) {
      impl->NewThreadStatusCfInfo(
          static_cast_with_check<ColumnFamilyHandleImpl>(h)->cfd());
    }
  } else {
    for (auto h : *handles) {
      delete h;
    }
    handles->clear();
    delete impl;
  }
  return s;
}

Status DBImplSecondary::ScanCompactionProgressFiles(
    CompactionProgressFilesScan* scan_result) {
  assert(scan_result != nullptr);
  scan_result->Clear();

  WriteOptions write_options(Env::IOActivity::kCompaction);
  IOOptions opts;
  Status s = WritableFileWriter::PrepareIOOptions(write_options, opts);
  if (!s.ok()) {
    return s;
  }

  std::vector<std::string> all_filenames;
  s = fs_->GetChildren(secondary_path_, opts, &all_filenames, nullptr /* dbg*/);
  if (!s.ok()) {
    return s;
  }

  for (const auto& filename : all_filenames) {
    if (filename == "." || filename == "..") {
      continue;
    }

    uint64_t number;
    FileType type;

    if (!ParseFileName(filename, &number, &type)) {
      continue;
    }

    // Categorize compaction progress files
    if (type == kCompactionProgressFile) {
      if (number > scan_result->latest_progress_timestamp) {
        // Found a newer progress file
        if (scan_result->HasLatestProgressFile()) {
          // Previous "latest" becomes "old"
          scan_result->old_progress_filenames.push_back(
              scan_result->latest_progress_filename.value());
        }
        scan_result->latest_progress_timestamp = number;
        scan_result->latest_progress_filename = filename;
      } else {
        // This is an older progress file
        scan_result->old_progress_filenames.push_back(filename);
      }
    } else if (type == kTempFile &&
               filename.find(kCompactionProgressFileNamePrefix) == 0) {
      // Temporary progress files
      scan_result->temp_progress_filenames.push_back(filename);
    } else if (type == kTableFile) {
      // Collect table file numbers for CleanupPhysicalCompactionOutputFiles
      scan_result->table_file_numbers.push_back(number);
    } else if (type == kBlobFile) {
      scan_result->blob_file_numbers.push_back(number);
    }
  }

  return Status::OK();
}

Status DBImplSecondary::DeleteCompactionProgressFiles(
    const std::vector<std::string>& filenames) {
  WriteOptions write_options(Env::IOActivity::kCompaction);
  IOOptions opts;
  Status s = WritableFileWriter::PrepareIOOptions(write_options, opts);
  if (!s.ok()) {
    return s;
  }

  for (const auto& filename : filenames) {
    std::string file_path = secondary_path_ + "/" + filename;
    Status delete_status = fs_->DeleteFile(file_path, opts, nullptr /* dbg */);
    if (!delete_status.ok()) {
      return delete_status;
    }
  }

  return Status::OK();
}

Status DBImplSecondary::CleanupOldAndTemporaryCompactionProgressFiles(
    bool preserve_latest, const CompactionProgressFilesScan& scan_result) {
  std::vector<std::string> filenames_to_delete;

  // Always delete old progress files
  filenames_to_delete.insert(filenames_to_delete.end(),
                             scan_result.old_progress_filenames.begin(),
                             scan_result.old_progress_filenames.end());

  // Always delete temp files
  filenames_to_delete.insert(filenames_to_delete.end(),
                             scan_result.temp_progress_filenames.begin(),
                             scan_result.temp_progress_filenames.end());

  // Conditionally delete latest file
  if (!preserve_latest && scan_result.HasLatestProgressFile()) {
    filenames_to_delete.push_back(scan_result.latest_progress_filename.value());
  }

  return DeleteCompactionProgressFiles(filenames_to_delete);
}

// Loads compaction progress from a file and cleans up extra output
// files. After loading the progress, this function identifies and deletes any
// SST and blob files in the output folder that are NOT tracked in the progress.
// This ensures consistency between the progress file and actual output files
// on disk.
Status DBImplSecondary::LoadCompactionProgressAndCleanupExtraOutputFiles(
    const std::string& compaction_progress_file_path,
    const CompactionProgressFilesScan& scan_result) {
  Status s = ParseCompactionProgressFile(compaction_progress_file_path,
                                         &compaction_progress_);
  if (s.ok()) {
    s = CleanupPhysicalCompactionOutputFiles(true /* preserve_tracked_files */,
                                             scan_result);
  }
  return s;
}

Status DBImplSecondary::ParseCompactionProgressFile(
    const std::string& compaction_progress_file_path,
    CompactionProgress* compaction_progress) {
  std::unique_ptr<FSSequentialFile> file;
  Status s = fs_->NewSequentialFile(compaction_progress_file_path,
                                    FileOptions(), &file, nullptr /* dbg */);
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<SequentialFileReader> file_reader(new SequentialFileReader(
      std::move(file), compaction_progress_file_path,
      immutable_db_options_.log_readahead_size, io_tracer_, {} /* listeners */,
      immutable_db_options_.rate_limiter.get()));

  Status reader_status;

  struct CompactionProgressReaderReporter : public log::Reader::Reporter {
    Status* status;
    explicit CompactionProgressReaderReporter(Status* s) : status(s) {}

    void Corruption(size_t /*bytes*/, const Status& s,
                    uint64_t /*log_number*/) override {
      if (status->ok()) {
        *status = s;
      }
    }

    void OldLogRecord(size_t /*bytes*/) override {
      // Ignore old records
    }
  } progress_reporter(&reader_status);

  log::Reader compaction_progress_reader(
      immutable_db_options_.info_log, std::move(file_reader),
      &progress_reporter, true /* checksum */, 0 /* log_num */);

  // LIMITATION: Only supports resuming single subcompaction
  SubcompactionProgressBuilder progress_builder;
  Slice slice;
  std::string record;

  while (compaction_progress_reader.ReadRecord(&slice, &record) &&
         reader_status.ok()) {
    VersionEdit edit;
    s = edit.DecodeFrom(slice);
    if (!s.ok()) {
      break;
    }

    bool res = progress_builder.ProcessVersionEdit(edit);
    if (!res) {
      break;
    }
  }

  if (!reader_status.ok()) {
    return reader_status;
  }

  if (!s.ok()) {
    return s;
  }

  if (progress_builder.HasAccumulatedSubcompactionProgress()) {
    compaction_progress->clear();
    compaction_progress->push_back(
        progress_builder.GetAccumulatedSubcompactionProgress());
  } else {
    s = Status::NotFound("No compaction progress was persisted yet");
  }

  return s;
}

Status DBImplSecondary::RenameCompactionProgressFile(
    const std::string& temp_file_path, std::string* final_file_path) {
  uint64_t current_time = env_->NowMicros();
  *final_file_path = CompactionProgressFileName(secondary_path_, current_time);

  WriteOptions write_options(Env::IOActivity::kCompaction);
  IOOptions opts;
  Status s = WritableFileWriter::PrepareIOOptions(write_options, opts);
  if (!s.ok()) {
    return s;
  }

  s = fs_->RenameFile(temp_file_path, *final_file_path, opts,
                      nullptr /* dbg */);

  return s;
}

Status DBImplSecondary::CleanupPhysicalCompactionOutputFiles(
    bool preserve_tracked_files,
    const CompactionProgressFilesScan& scan_result) {
  std::unordered_set<uint64_t> table_files_to_preserve;
  std::unordered_set<uint64_t> blob_files_to_preserve;

  if (preserve_tracked_files) {
    for (const auto& subcompaction_progress : compaction_progress_) {
      for (const auto& file_metadata :
           subcompaction_progress.output_level_progress.GetOutputFiles()) {
        table_files_to_preserve.insert(file_metadata.fd.GetNumber());
      }
      for (const auto& file_metadata :
           subcompaction_progress.proximal_output_level_progress
               .GetOutputFiles()) {
        table_files_to_preserve.insert(file_metadata.fd.GetNumber());
      }
      for (const auto& addition : subcompaction_progress.blob_file_additions) {
        blob_files_to_preserve.insert(addition.GetBlobFileNumber());
      }
    }
  }

  WriteOptions write_options(Env::IOActivity::kCompaction);
  IOOptions opts;
  Status s = WritableFileWriter::PrepareIOOptions(write_options, opts);
  if (!s.ok()) {
    return s;
  }

  for (uint64_t file_number : scan_result.table_file_numbers) {
    bool should_delete =
        !preserve_tracked_files || (table_files_to_preserve.find(file_number) ==
                                    table_files_to_preserve.end());

    if (should_delete) {
      std::string file_path = MakeTableFileName(secondary_path_, file_number);
      Status delete_status =
          fs_->DeleteFile(file_path, opts, nullptr /* dbg */);
      if (!delete_status.ok()) {
        return delete_status;
      }
    }
  }

  for (uint64_t file_number : scan_result.blob_file_numbers) {
    const bool should_delete =
        !preserve_tracked_files || blob_files_to_preserve.find(file_number) ==
                                       blob_files_to_preserve.end();
    if (should_delete) {
      TEST_SYNC_POINT_CALLBACK(
          "DBImplSecondary::CleanupPhysicalCompactionOutputFiles::BlobFile",
          &file_number);
      const std::string file_path = BlobFileName(secondary_path_, file_number);
      Status delete_status =
          fs_->DeleteFile(file_path, opts, nullptr /* dbg */);
      if (!delete_status.ok()) {
        return delete_status;
      }
    }
  }

  return Status::OK();
}

Status DBImplSecondary::InitializeCompactionWorkspace(
    bool allow_resumption, bool resumption_requested,
    std::unique_ptr<FSDirectory>* output_dir,
    std::unique_ptr<log::Writer>* compaction_progress_writer) {
  // Create output directory if it doest exist yet
  Status s = CreateAndNewDirectory(fs_.get(), secondary_path_, output_dir);
  if (!s.ok()) {
    return s;
  }

  if (allow_resumption) {
    s = PrepareCompactionProgressState();
    if (!s.ok()) {
      return s;
    }
    return FinalizeCompactionProgressWriter(compaction_progress_writer);
  }

  if (resumption_requested) {
    // Resumption was requested by the caller but has been disabled internally
    // (see CompactWithoutInstallation: incompatible with output hash
    // verification). The caller therefore did not empty output_directory (it
    // expects the resumption path to own that state), so honor the
    // OpenAndCompactOptions::allow_resumption=true fallback contract by
    // cleaning any leftover progress and output files here, starting the fresh
    // compaction from a clean directory. Without this, output files left by a
    // previously interrupted attempt collide with the file numbers the fresh
    // compaction reuses.
    CompactionProgressFilesScan scan_result;
    s = ScanCompactionProgressFiles(&scan_result);
    if (!s.ok()) {
      return s;
    }
    s = CleanupOldAndTemporaryCompactionProgressFiles(
        /*preserve_latest=*/false, scan_result);
    if (!s.ok()) {
      return s;
    }
    s = HandleInvalidOrNoCompactionProgress(
        /*compaction_progress_file_path=*/std::nullopt, scan_result);
  }

  return s;
}

// PrepareCompactionProgressState() manages compaction progress files and output
// files to ensure a clean, consistent state for resuming or starting fresh
// compaction.
//
// PRECONDITION:
// - This function is ONLY called when allow_resumption = true
// - The caller wants resumption support for this compaction attempt
//
// FILE SYSTEM STATE (before entering this function):
// - 0 or more compaction progress files may exist in `secondary_path_`:
//   * Latest progress file (from the most recent compaction attempt)
//   * Older progress files (left by crashing during a previous
//     InitializeCompactionWorkspace() call)
//   * Temporary progress files (left by crashing during a previous
//     InitializeCompactionWorkspace() call)
// - 0 or more compaction output files may exist in `secondary_path_`
//
// POSTCONDITIONS (after this function):
// - IF the latest progress file exists AND it parses successfully AND
//   actually contains valid compaction progress:
//   * Exactly one latest progress file remains
//   * All older and temporary compaction progress files are deleted
//   * All corresponding compaction output files are preserved
//   * All extra compaction output files are deleted (files left by
//   compaction
//     crashing before persisting the progress)
//   * Result: Ready to resume compaction from the saved progress
// - OTHERWISE (no latest progress file OR it fails to parse OR it's
// invalid):
//   * ALL compaction progress files are deleted (latest + older +
//   temporary)
//   * ALL compaction output files are deleted
//   * Result: Ready to start fresh compaction (despite allow_resumption =
//   true, we cannot resume because there's no valid progress to resume from)
//
// ERROR HANDLING:
// - ON ERROR (if any of the postconditions cannot be achieved):
//   * Function returns error status
//   * File system may be left in a partially modified state
//   * Caller should manually clean up secondary_path_ before retrying
//   * Subsequent OpenAndCompact() calls to this clean secondary_path_ will
//     effectively start fresh compaction
Status DBImplSecondary::PrepareCompactionProgressState() {
  Status s;

  // STEP 1: Scan directory ONCE (includes progress, table, and blob files)
  CompactionProgressFilesScan scan_result;
  s = ScanCompactionProgressFiles(&scan_result);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                    "Encountered error when scanning for compaction "
                    "progress files: %s",
                    s.ToString().c_str());
    return s;
  }

  std::optional<std::string> latest_progress_file =
      scan_result.latest_progress_filename;

  // STEP 2: Determine if we should resume
  bool should_resume = false;
  if (latest_progress_file.has_value()) {
    should_resume = true;
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "Did not find any latest compaction progress file. "
                   "Will perform clean up to start fresh compaction");
  }

  // STEP 3: Cleanup using pre-scanned results
  if (should_resume) {
    // Keep latest, delete old/temp
    s = CleanupOldAndTemporaryCompactionProgressFiles(
        true /* preserve_latest */, scan_result);
  } else {
    // Delete everything including latest
    s = CleanupOldAndTemporaryCompactionProgressFiles(
        false /* preserve_latest */, scan_result);
    latest_progress_file.reset();
  }

  if (!s.ok()) {
    ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                    "Failed to clean up compaction progress file(s): %s. "
                    "Will fail the compaction",
                    s.ToString().c_str());
    return s;
  }

  // STEP 4: Load progress if resuming
  if (latest_progress_file.has_value()) {
    uint64_t timestamp = scan_result.latest_progress_timestamp;

    std::string compaction_progress_file_path =
        CompactionProgressFileName(secondary_path_, timestamp);

    s = LoadCompactionProgressAndCleanupExtraOutputFiles(
        compaction_progress_file_path, scan_result);

    if (!s.ok()) {
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "Failed to load the latest compaction "
                     "progress from %s: %s. Will perform clean up "
                     "to start fresh compaction",
                     latest_progress_file.value().c_str(),
                     s.ToString().c_str());
      return HandleInvalidOrNoCompactionProgress(compaction_progress_file_path,
                                                 scan_result);
    }

    ROCKS_LOG_DEBUG(
        immutable_db_options_.info_log,
        "Loaded compaction progress with %zu subcompaction(s) from %s",
        compaction_progress_.size(), compaction_progress_file_path.c_str());
    return s;
  } else {
    return HandleInvalidOrNoCompactionProgress(
        std::nullopt /* compaction_progress_file_path */, scan_result);
  }
}

uint64_t DBImplSecondary::CalculateResumedCompactionBytes(
    const CompactionProgress& compaction_progress) const {
  uint64_t total_resumed_bytes = 0;

  for (const auto& subcompaction_progress : compaction_progress) {
    for (const auto& file_meta :
         subcompaction_progress.output_level_progress.GetOutputFiles()) {
      total_resumed_bytes += file_meta.fd.file_size;
    }

    for (const auto& file_meta :
         subcompaction_progress.proximal_output_level_progress
             .GetOutputFiles()) {
      total_resumed_bytes += file_meta.fd.file_size;
    }
  }

  return total_resumed_bytes;
}

Status DBImplSecondary::HandleInvalidOrNoCompactionProgress(
    const std::optional<std::string>& compaction_progress_file_path,
    const CompactionProgressFilesScan& scan_result) {
  compaction_progress_.clear();

  Status s;
  if (compaction_progress_file_path.has_value()) {
    WriteOptions write_options(Env::IOActivity::kCompaction);
    IOOptions opts;
    s = WritableFileWriter::PrepareIOOptions(write_options, opts);
    if (s.ok()) {
      s = fs_->DeleteFile(compaction_progress_file_path.value(), opts,
                          nullptr /* dbg */);
    }
    if (!s.ok()) {
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "Failed to remove invalid progress file: %s",
                      s.ToString().c_str());
      return s;
    }
  }

  s = CleanupPhysicalCompactionOutputFiles(false /* preserve_tracked_files */,
                                           scan_result);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                    "Failed to cleanup existing compaction output files: %s",
                    s.ToString().c_str());
    return s;
  }

  return Status::OK();
}

Status DBImplSecondary::CompactWithoutInstallation(
    const OpenAndCompactOptions& options, ColumnFamilyHandle* cfh,
    const CompactionServiceInput& input, CompactionServiceResult* result) {
  if (options.canceled && options.canceled->load(std::memory_order_acquire)) {
    return Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }

  std::unique_ptr<FSDirectory> output_dir;
  std::unique_ptr<log::Writer> compaction_progress_writer;

  InstrumentedMutexLock l(&mutex_);

  auto cfd = static_cast_with_check<ColumnFamilyHandleImpl>(cfh)->cfd();
  if (!cfd) {
    return Status::InvalidArgument("Cannot find column family" +
                                   cfh->GetName());
  }
  Status s;

  // DB::OpenAndCompact() runs this compaction in a secondary instance, which
  // is otherwise read-only, so SanitizeCfOptions() zeroed preserve/preclude.
  // But this compaction writes output files and decides data placement, so it
  // needs the primary's values: for per-key placement, for which sequence
  // numbers may be zeroed, and for the seqno->time mapping written to output
  // files. Only this local copy is changed; the CF's own options stay
  // sanitized as a read-only DB requires.
  MutableCFOptions mutable_cf_options = cfd->GetLatestMutableCFOptions();
  mutable_cf_options.preserve_internal_time_seconds =
      input.preserve_internal_time_seconds;
  mutable_cf_options.preclude_last_level_data_seconds =
      input.preclude_last_level_data_seconds;

  // TODO(hx235): Resuming compaction is currently incompatible with
  // output hash verification (enabled via paranoid_file_checks=true or
  // verify_output_flags containing kVerifyIteration) because resumed compaction
  // will lose the hash computed before interruption.
  // Potential solutions:
  // 1. Persist the hash state: Before interruption, save the current hash value
  //    of each output file to disk, allowing validation to continue correctly
  //    after resumption.
  // 2. Immediate verification: Move output verification to happen
  //    immediately after each output file is created and closed, eliminating
  //    the need to maintain hash state across resumption boundaries.
  bool output_hash_verification_enabled =
      mutable_cf_options.paranoid_file_checks ||
      !!(mutable_cf_options.verify_output_flags &
         VerifyOutputFlags::kVerifyIteration);

  bool allow_resumption =
      options.allow_resumption && !output_hash_verification_enabled;

  if (options.allow_resumption && output_hash_verification_enabled) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "Resume compaction configured but disabled due to "
                   "incompatibility with output hash verification "
                   "(paranoid_file_checks=true or verify_output_flags "
                   "containing kVerifyIteration)");
  }

  mutex_.Unlock();

  s = InitializeCompactionWorkspace(
      allow_resumption, /*resumption_requested=*/options.allow_resumption,
      &output_dir, &compaction_progress_writer);

  mutex_.Lock();

  if (!s.ok()) {
    return s;
  }

  std::unordered_set<uint64_t> input_set;
  for (const auto& file_name : input.input_files) {
    input_set.insert(TableFileNameToNumber(file_name));
  }

  auto* version = cfd->current();

  ColumnFamilyMetaData cf_meta;
  version->GetColumnFamilyMetaData(&cf_meta);

  VersionStorageInfo* vstorage = version->storage_info();

  CompactionOptions comp_options;
  comp_options.compression = kDisableCompressionOption;
  comp_options.output_file_size_limit = MaxFileSizeForLevel(
      mutable_cf_options, input.output_level, cfd->ioptions().compaction_style,
      vstorage->base_level(),
      cfd->ioptions().level_compaction_dynamic_level_bytes);

  std::vector<CompactionInputFiles> input_files;
  s = cfd->compaction_picker()->GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage, comp_options);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(
        immutable_db_options_.info_log,
        "GetCompactionInputsFromFileNumbers() failed - %s.\n DebugString: %s",
        s.ToString().c_str(), version->DebugString(/*hex=*/true).c_str());
    return s;
  }

  const int job_id = next_job_id_.fetch_add(1);
  JobContext job_context(job_id, true /*create_superversion*/);
  std::vector<SequenceNumber> snapshots = input.snapshots;

  // TODO - snapshot_checker support in Remote Compaction. When implemented,
  // add a resumable remote test for kKeepSDAndClearPut.
  job_context.InitSnapshotContext(/*checker=*/nullptr,
                                  /*managed_snapshot=*/nullptr,
                                  kMaxSequenceNumber, std::move(snapshots));

  // Ensure FileMetaData stats (num_entries, num_range_deletions) are
  // initialized for all input files. These stats are not persisted in the
  // MANIFEST and are loaded lazily from table properties. The Compaction
  // constructor's FilterInputsForCompactionIterator() relies on
  // FileIsStandAloneRangeTombstone() which needs these fields to be populated.
  // Without this, the remote worker may not filter the same files as the
  // primary host, leading to input record count verification failures.
  {
    const ReadOptions read_options(Env::IOActivity::kCompaction);
    for (const auto& level_files : input_files) {
      for (FileMetaData* file_meta : level_files.files) {
        version->MaybeInitializeFileMetaData(read_options, file_meta);
      }
    }
  }

  // TODO - consider serializing the entire Compaction object and using it as
  // input instead of recreating it in the remote worker
  std::unique_ptr<Compaction> c;
  assert(cfd->compaction_picker());
  std::optional<SequenceNumber> earliest_snapshot = std::nullopt;
  // Standalone Range Deletion Optimization is only supported in Universal
  // Compactions - https://github.com/meta/rocksdb/pull/13078
  if (cfd->GetLatestCFOptions().compaction_style ==
      CompactionStyle::kCompactionStyleUniversal) {
    earliest_snapshot = !job_context.snapshot_seqs.empty()
                            ? job_context.snapshot_seqs.front()
                            : kMaxSequenceNumber;
  }
  c.reset(cfd->compaction_picker()->PickCompactionForCompactFiles(
      comp_options, input_files, input.output_level, vstorage,
      mutable_cf_options, mutable_db_options_, 0, earliest_snapshot,
      job_context.snapshot_checker));
  assert(c != nullptr);
  c->FinalizeInputInfo(version);

  // The worker re-derives per-key placement from its own MANIFEST view, which
  // can be newer than the primary's input version. The primary installs
  // proximal-level outputs according to its own decisions, so decline on any
  // disagreement and let the CompactionService fall back to local compaction.
  // A proximal_level of 0 means an older primary that sent no decisions.
  bool placement_mismatch =
      input.proximal_level != 0 &&
      (c->GetProximalLevel() != input.proximal_level ||
       static_cast<int>(c->GetProximalOutputRangeType()) !=
           input.proximal_output_range_type ||
       c->GetKeepInLastLevelThroughSeqno() !=
           input.keep_in_last_level_through_seqno);
  TEST_SYNC_POINT_CALLBACK(
      "DBImplSecondary::CompactWithoutInstallation:PerKeyPlacementCheck",
      &placement_mismatch);
  if (placement_mismatch) {
    ROCKS_LOG_WARN(
        immutable_db_options_.info_log,
        "[%s] Remote compaction per-key placement differs from the "
        "primary's (proximal level %d vs %d, range type %d vs %d, "
        "keep in last level through seqno %" PRIu64 " vs %" PRIu64
        "); declining so the job can fall back to local compaction.",
        cfd->GetName().c_str(), c->GetProximalLevel(), input.proximal_level,
        static_cast<int>(c->GetProximalOutputRangeType()),
        input.proximal_output_range_type, c->GetKeepInLastLevelThroughSeqno(),
        input.keep_in_last_level_through_seqno);
    s = Status::Incomplete(
        "Remote compaction worker's per-key placement differs from the "
        "primary's");
    c->ReleaseCompactionFiles(s);
    return s;
  }

  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());

  // use primary host's db_id for running the compaction, but db_session_id is
  // using the local one, which is to make sure the unique id is unique from
  // the remote compactors. Because the id is generated from db_id,
  // db_session_id and orig_file_number, unlike the local compaction, remote
  // compaction cannot guarantee the uniqueness of orig_file_number, the file
  // number is only assigned when compaction is done.
  CompactionServiceCompactionJob compaction_job(
      job_id, c.get(), immutable_db_options_, mutable_db_options_,
      file_options_for_compaction_, versions_.get(), &shutting_down_,
      &log_buffer, output_dir.get(), stats_, &mutex_, &error_handler_,
      &job_context, table_cache_, &event_logger_, dbname_, io_tracer_,
      options.canceled ? *options.canceled : kManualCompactionCanceledFalse_,
      input.db_id, db_session_id_, secondary_path_, input, result);

  compaction_job.Prepare(compaction_progress_,
                         compaction_progress_writer.get());

  mutex_.Unlock();
  s = compaction_job.Run();
  mutex_.Lock();

  // These cleanup functions handle metadata and state cleanup only and
  // not the physical files
  compaction_job.io_status().PermitUncheckedError();
  compaction_job.CleanupCompaction();
  c->ReleaseCompactionFiles(s);
  c.reset();

  TEST_SYNC_POINT_CALLBACK("DBImplSecondary::CompactWithoutInstallation::End",
                           &s);

  if (!compaction_progress_.empty() && s.ok()) {
    uint64_t total_resumed_bytes =
        CalculateResumedCompactionBytes(compaction_progress_);

    if (total_resumed_bytes > 0 &&
        immutable_db_options_.statistics != nullptr) {
      RecordTick(immutable_db_options_.statistics.get(),
                 REMOTE_COMPACT_RESUMED_BYTES, total_resumed_bytes);
    }
  }

  result->status = s;
  return s;
}

Status DB::OpenAndCompact(
    const OpenAndCompactOptions& options, const std::string& name,
    const std::string& output_directory, const std::string& input,
    std::string* output,
    const CompactionServiceOptionsOverride& override_options) {
  // Check for cancellation
  if (options.canceled && options.canceled->load(std::memory_order_acquire)) {
    return Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }

  // 1. Deserialize Compaction Input
  CompactionServiceInput compaction_input;
  Status s = CompactionServiceInput::Read(input, &compaction_input);
  if (!s.ok()) {
    return s;
  }

  // 2. Load the options
  DBOptions base_db_options;
  ConfigOptions config_options;
  config_options.env = override_options.env;
  config_options.ignore_unknown_options = true;
  std::vector<ColumnFamilyDescriptor> all_column_families;

  TEST_SYNC_POINT_CALLBACK(
      "DBImplSecondary::OpenAndCompact::BeforeLoadingOptions:0",
      &compaction_input.options_file_number);
  TEST_SYNC_POINT("DBImplSecondary::OpenAndCompact::BeforeLoadingOptions:1");
  std::string options_file_name =
      OptionsFileName(name, compaction_input.options_file_number);

  s = LoadOptionsFromFile(config_options, options_file_name, &base_db_options,
                          &all_column_families);
  if (!s.ok()) {
    return s;
  }

  // 3. Options to Override
  // Override serializable configurations from override_options.options_map
  DBOptions db_options;
  s = GetDBOptionsFromMap(config_options, base_db_options,
                          override_options.options_map, &db_options);
  if (!s.ok()) {
    return s;
  }

  // Override options that are directly set as shared ptrs in
  // CompactionServiceOptionsOverride
  db_options.env = override_options.env;
  db_options.file_checksum_gen_factory =
      override_options.file_checksum_gen_factory;
  db_options.statistics = override_options.statistics;
  db_options.listeners = override_options.listeners;
  db_options.compaction_service = nullptr;
  db_options.info_log = override_options.info_log;

  const std::string output_path = RemoteCompactionJobDir(
      name, db_options.use_session_tmp_dir_for_remote_compaction,
      output_directory);

  // 4. Filter CFs that are needed for OpenAndCompact()
  // We do not need to open all column families for the remote compaction.
  // Only open default CF + target CF. If target CF == default CF, we will open
  // just the default CF (Due to current limitation, DB cannot open without the
  // default CF)
  std::vector<ColumnFamilyDescriptor> column_families;
  for (auto& cf : all_column_families) {
    if (cf.name == compaction_input.cf_name) {
      ColumnFamilyOptions cf_options;
      // Override serializable configurations from override_options.options_map
      s = GetColumnFamilyOptionsFromMap(config_options, cf.options,
                                        override_options.options_map,
                                        &cf_options);
      if (!s.ok()) {
        return s;
      }
      cf.options = std::move(cf_options);

      // Override options that are directly set as shared ptrs in
      // CompactionServiceOptionsOverride
      cf.options.comparator = override_options.comparator;
      cf.options.merge_operator = override_options.merge_operator;
      cf.options.compaction_filter = override_options.compaction_filter;
      cf.options.compaction_filter_factory =
          override_options.compaction_filter_factory;
      cf.options.prefix_extractor = override_options.prefix_extractor;
      cf.options.table_factory = override_options.table_factory;
      cf.options.sst_partitioner_factory =
          override_options.sst_partitioner_factory;
      cf.options.table_properties_collector_factories =
          override_options.table_properties_collector_factories;

      column_families.emplace_back(cf);
    } else if (cf.name == kDefaultColumnFamilyName) {
      column_families.emplace_back(cf);
    }
  }

  // 5. Open db As Secondary (skip WAL recovery -- remote compaction only
  //    needs LSM state from MANIFEST, not memtable data from WAL replay)
  //
  // A non-zero min_manifest_file_number means the primary provided a MANIFEST
  // floor (DBOptions::remote_compaction_manifest_floor was on when it scheduled
  // the job). Its presence couples on both (a) "trust the MANIFEST" recovery
  // and (b) the floor check below. Absence -> the worker uses the original
  // point-in-time recovery with no floor check (kill switch off, or an older
  // primary).
  const bool floor_provided = compaction_input.min_manifest_file_number != 0;
  std::unique_ptr<DB> db;
  std::vector<ColumnFamilyHandle*> handles;
  const uint64_t db_open_start_micros = db_options.env->NowMicros();
  // This opens the *live* primary's directory read-only, and the primary keeps
  // rotating its MANIFEST underneath us: each rotation writes a new MANIFEST
  // and renames a freshly written CURRENT over the old one. POSIX keeps an
  // already-opened CURRENT/MANIFEST readable after such a replacement, but
  // filesystems without read-after-unlink semantics (remote or object stores)
  // report the replaced object as gone instead (Status::PathNotFound /
  // NotFound, or Status::TryAgain from
  // ReactiveVersionSet::MaybeSwitchManifest for the MANIFEST equivalent). That
  // is a transient race, not a broken DB: the next attempt reads the
  // replacement. Retry a bounded number of times so a concurrent rotation does
  // not fail the compaction -- a CompactionService that does not fall back to a
  // local compaction turns such a failure into a primary background error.
  for (uint32_t retry_count = 0;; ++retry_count) {
    s = DBImplSecondary::OpenAsSecondaryImpl(
        db_options, name, output_path, column_families, &handles, &db,
        /*recover_wal=*/false,
        /*trust_manifest_recovery=*/floor_provided);
    if (s.ok() || !(s.IsTryAgain() || s.IsPathNotFound() || s.IsNotFound()) ||
        retry_count == options.max_secondary_open_retries) {
      break;
    }
    ROCKS_LOG_WARN(db_options.info_log,
                   "OpenAndCompact: secondary open of %s failed with %s "
                   "(retry %" PRIu32 " of %" PRIu32
                   "); the primary may have replaced "
                   "CURRENT/MANIFEST concurrently, retrying",
                   name.c_str(), s.ToString().c_str(), retry_count + 1,
                   options.max_secondary_open_retries);
  }
  RecordTimeToHistogram(db_options.statistics.get(),
                        OPEN_AND_COMPACT_DB_OPEN_MICROS,
                        db_options.env->NowMicros() - db_open_start_micros);
  if (!s.ok()) {
    return s;
  }
  assert(db);

  TEST_SYNC_POINT_CALLBACK(
      "DBImplSecondary::OpenAndCompact::AfterOpenAsSecondary:0", db.get());

  // 5b. Manifest floor check. Refuse to reconstruct the compaction against an
  // older MANIFEST view than the primary scheduled from (e.g. an
  // eventually-consistent filesystem returning a stale CURRENT, or a truncated
  // MANIFEST): an older LSM shape could wrongly drop keys that should be kept.
  // Accept-equal-or-later is safe by monotonicity while the inputs stay locked;
  // only an older view is unsafe. On rejection, return Status::Incomplete so
  // the CompactionService implementation can fall back to a local compaction
  // (kUseLocal) rather than install a possibly-incorrect result.
  if (floor_provided) {
    VersionSet* recovered_versions =
        static_cast_with_check<DBImplSecondary>(db.get())->GetVersionSet();
    const uint64_t recovered_number =
        recovered_versions->manifest_file_number();
    // Compare the maximal valid prefix of the recovered MANIFEST (not the
    // reader's read-to-EOF high-water mark, which can include tolerated tail
    // garbage: a corrupt/torn trailing record or a partial atomic group at
    // EOF). This way a tolerated corrupt tail is fine as long as the
    // actually-installed valid prefix reaches the floor. Both sides are byte
    // sizes of the same append-only, uniquely-numbered MANIFEST, so equal
    // number + recovered_valid_size >= min means the worker installed a
    // same-or-newer view; anything short (older/truncated/torn/partial) is
    // below the floor and rejected.
    const uint64_t recovered_valid_size =
        recovered_versions->manifest_recovery_maximal_valid_size();
    bool below_floor =
        recovered_number < compaction_input.min_manifest_file_number ||
        (recovered_number == compaction_input.min_manifest_file_number &&
         recovered_valid_size < compaction_input.min_manifest_file_size);
    TEST_SYNC_POINT_CALLBACK(
        "DBImplSecondary::OpenAndCompact::ManifestFloorCheck", &below_floor);
    if (below_floor) {
      ROCKS_LOG_WARN(
          db_options.info_log,
          "Remote compaction recovered an older MANIFEST view (file number "
          "%" PRIu64 " valid size %" PRIu64
          ") than the primary scheduled from "
          "(min file number %" PRIu64 " size %" PRIu64
          "); declining so the job can fall back to local compaction.",
          recovered_number, recovered_valid_size,
          compaction_input.min_manifest_file_number,
          compaction_input.min_manifest_file_size);
      for (auto& handle : handles) {
        delete handle;
      }
      db.reset();
      return Status::Incomplete(
          "Remote compaction worker recovered an older MANIFEST view than the "
          "primary scheduled from");
    }
  }

  // 6. Find the handle of the Column Family that this will compact
  ColumnFamilyHandle* cfh = nullptr;
  for (auto* handle : handles) {
    if (compaction_input.cf_name == handle->GetName()) {
      cfh = handle;
      break;
    }
  }
  assert(cfh);

  // 7. Run the compaction without installation.
  // Output will be stored under the DB-owned staging directory.
  CompactionServiceResult compaction_result;
  DBImplSecondary* db_secondary =
      static_cast_with_check<DBImplSecondary>(db.get());
  s = db_secondary->CompactWithoutInstallation(options, cfh, compaction_input,
                                               &compaction_result);

  // 8. Serialize the result
  Status serialization_status = compaction_result.Write(output);

  // 9. Close the db and return
  for (auto& handle : handles) {
    delete handle;
  }
  db.reset();
  if (s.ok()) {
    return serialization_status;
  } else {
    serialization_status.PermitUncheckedError();
  }
  return s;
}

Status DB::OpenAndCompact(
    const std::string& name, const std::string& output_directory,
    const std::string& input, std::string* output,
    const CompactionServiceOptionsOverride& override_options) {
  return OpenAndCompact(OpenAndCompactOptions(), name, output_directory, input,
                        output, override_options);
}

Status DBImplSecondary::CreateCompactionProgressWriter(
    const std::string& file_path,
    std::unique_ptr<log::Writer>* compaction_progress_writer) {
  std::unique_ptr<FSWritableFile> file;
  Status s =
      fs_->NewWritableFile(file_path, FileOptions(), &file, nullptr /* dbg */);
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(file), file_path, FileOptions()));

  compaction_progress_writer->reset(
      new log::Writer(std::move(file_writer), 0 /* log_number */,
                      false /* recycle_log_files */));

  return Status::OK();
}

Status DBImplSecondary::PersistInitialCompactionProgress(
    log::Writer* compaction_progress_writer,
    CompactionProgress* compaction_progress) {
  assert(compaction_progress_writer);
  assert(compaction_progress);

  // LIMITATION: Only supports resuming single subcompaction
  assert(compaction_progress->size() == 1);
  SubcompactionProgress& subcompaction_progress = (*compaction_progress)[0];

  VersionEdit edit;
  edit.SetSubcompactionProgress(subcompaction_progress);

  std::string record;
  if (!edit.EncodeTo(&record)) {
    return Status::IOError("Failed to encode the initial compaction progress");
  }

  WriteOptions write_options(Env::IOActivity::kCompaction);
  Status s = compaction_progress_writer->AddRecord(write_options, record);
  if (!s.ok()) {
    return s;
  }
  IOOptions opts;
  s = WritableFileWriter::PrepareIOOptions(write_options, opts);
  if (!s.ok()) {
    return s;
  }

  s = immutable_db_options_.use_fsync
          ? compaction_progress_writer->file()->Fsync(opts)
          : compaction_progress_writer->file()->Sync(opts);

  if (s.ok()) {
    subcompaction_progress.output_level_progress
        .UpdateLastPersistedOutputFilesCount();
    subcompaction_progress.proximal_output_level_progress
        .UpdateLastPersistedOutputFilesCount();
    subcompaction_progress.UpdateLastPersistedBlobFileAdditionsCount();
  }

  return s;
}

Status DBImplSecondary::HandleCompactionProgressWriterCreationFailure(
    const std::string& temp_file_path, const std::string& final_file_path,
    std::unique_ptr<log::Writer>* compaction_progress_writer) {
  compaction_progress_writer->reset();

  const std::vector<std::string> paths_to_delete = {final_file_path,
                                                    temp_file_path};

  Status s;
  for (const auto& file_path : paths_to_delete) {
    WriteOptions write_options(Env::IOActivity::kCompaction);
    IOOptions opts;
    s = WritableFileWriter::PrepareIOOptions(write_options, opts);
    if (s.ok()) {
      s = fs_->DeleteFile(file_path, opts, nullptr /* dbg */);
    }

    if (!s.ok()) {
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "Failed to cleanup the compaction progress file "
                      "during writer creation failure: %s",
                      s.ToString().c_str());
      return s;
    }
  }

  return s;
}

Status DBImplSecondary::FinalizeCompactionProgressWriter(
    std::unique_ptr<log::Writer>* compaction_progress_writer) {
  uint64_t timestamp = env_->NowMicros();
  const std::string temp_file_path =
      TempCompactionProgressFileName(secondary_path_, timestamp);

  Status s = CreateCompactionProgressWriter(temp_file_path,
                                            compaction_progress_writer);
  if (!s.ok()) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "Failed to create compaction progress writer at "
                   "temp path %s: %s. Will perform clean up "
                   "to start compaction without progress persistence",
                   temp_file_path.c_str(), s.ToString().c_str());
    return HandleCompactionProgressWriterCreationFailure(
        temp_file_path, "" /* final_file_path */, compaction_progress_writer);
  }

  compaction_progress_writer->reset();

  std::string final_file_path;
  s = RenameCompactionProgressFile(temp_file_path, &final_file_path);

  if (!s.ok()) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "Failed to rename temporary compaction progress "
                   "file from %s to %s: %s.  Will perform clean up "
                   "to start compaction without progress persistence",
                   temp_file_path.c_str(), final_file_path.c_str(),
                   s.ToString().c_str());
    return HandleCompactionProgressWriterCreationFailure(
        temp_file_path, final_file_path, compaction_progress_writer);
  }

  s = CreateCompactionProgressWriter(final_file_path,
                                     compaction_progress_writer);
  if (!s.ok()) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "Failed to create the final compaction progress "
                   "writer: %s. Will attempt clean to start the compaction "
                   "without progress persistence",
                   s.ToString().c_str());
    return HandleCompactionProgressWriterCreationFailure(
        "" /* temp_file_path */, final_file_path, compaction_progress_writer);
  }

  // The resumed progress must be written through the final writer: reopening
  // the final file with NewWritableFile() truncates it, and later checkpoints
  // only persist output files added after this record.
  if (!compaction_progress_.empty()) {
    s = PersistInitialCompactionProgress(compaction_progress_writer->get(),
                                         &compaction_progress_);
    if (!s.ok()) {
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "Failed to persist the initial compaction "
                     "progress: %s. Will perform clean up "
                     "to start compaction without progress persistence",
                     s.ToString().c_str());
      return HandleCompactionProgressWriterCreationFailure(
          "" /* temp_file_path */, final_file_path, compaction_progress_writer);
    }
  }

  ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                  "Finalized compaction progress writer onto %s",
                  final_file_path.c_str());

  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE

// Generate the regular and coroutine versions of the functions defined in
// db_impl_secondary_sync_and_async.h by including it twice. The macros expand
// differently based on whether WITH_COROUTINES or WITHOUT_COROUTINES is
// defined.
// clang-format off
#define WITHOUT_COROUTINES
#include "db/db_impl/db_impl_secondary_sync_and_async.h"
#undef WITHOUT_COROUTINES
#define WITH_COROUTINES
#include "db/db_impl/db_impl_secondary_sync_and_async.h"
#undef WITH_COROUTINES
// clang-format on
