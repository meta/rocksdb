//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/transactions/write_unprepared_txn.h"

#include "db/db_impl/db_impl.h"
#include "util/cast_util.h"
#include "utilities/transactions/write_unprepared_txn_db.h"
#include "utilities/write_batch_with_index/write_batch_with_index_internal.h"

namespace ROCKSDB_NAMESPACE {

bool WriteUnpreparedTxnReadCallback::IsVisibleFullCheck(SequenceNumber seq) {
  // Since unprep_seqs maps prep_seq => prepare_batch_cnt, to check if seq is
  // in unprep_seqs, we have to check if seq is equal to prep_seq or any of
  // the prepare_batch_cnt seq nums after it.
  //
  // TODO(lth): Can be optimized with std::lower_bound if unprep_seqs is
  // large.
  for (const auto& it : unprep_seqs_) {
    if (it.first <= seq && seq < it.first + it.second) {
      return true;
    }
  }

  bool snap_released = false;
  auto ret =
      db_->IsInSnapshot(seq, wup_snapshot_, min_uncommitted_, &snap_released);
  assert(!snap_released || backed_by_snapshot_ == kUnbackedByDBSnapshot);
  snap_released_ |= snap_released;
  return ret;
}

WriteUnpreparedTxn::WriteUnpreparedTxn(WriteUnpreparedTxnDB* txn_db,
                                       const WriteOptions& write_options,
                                       const TransactionOptions& txn_options)
    : WritePreparedTxn(txn_db, write_options, txn_options),
      wupt_db_(txn_db),
      last_log_number_(0),
      recovered_txn_(false),
      largest_validated_seq_(0) {
  if (txn_options.write_batch_flush_threshold < 0) {
    write_batch_flush_threshold_ =
        txn_db_impl_->GetTxnDBOptions().default_write_batch_flush_threshold;
  } else {
    write_batch_flush_threshold_ = txn_options.write_batch_flush_threshold;
  }
}

WriteUnpreparedTxn::~WriteUnpreparedTxn() {
  if (!unprep_seqs_.empty()) {
    assert(log_number_ > 0);
    assert(GetId() > 0);
    assert(!name_.empty());

    // We should rollback regardless of GetState, but some unit tests that
    // test crash recovery run the destructor assuming that rollback does not
    // happen, so that rollback during recovery can be exercised.
    if (GetState() == STARTED || GetState() == LOCKS_STOLEN) {
      auto s = RollbackInternal();
      assert(s.ok());
      if (!s.ok()) {
        ROCKS_LOG_FATAL(
            wupt_db_->info_log_,
            "Rollback of WriteUnprepared transaction failed in destructor: %s",
            s.ToString().c_str());
      }
      dbimpl_->logs_with_prep_tracker()->MarkLogAsHavingPrepSectionFlushed(
          log_number_);
    }
  }

  // Clear the tracked locks so that ~PessimisticTransaction does not
  // try to unlock keys for recovered transactions.
  if (recovered_txn_) {
    tracked_locks_->Clear();
  }
}

void WriteUnpreparedTxn::Initialize(const TransactionOptions& txn_options) {
  PessimisticTransaction::Initialize(txn_options);
  if (txn_options.write_batch_flush_threshold < 0) {
    write_batch_flush_threshold_ =
        txn_db_impl_->GetTxnDBOptions().default_write_batch_flush_threshold;
  } else {
    write_batch_flush_threshold_ = txn_options.write_batch_flush_threshold;
  }

  unprep_seqs_.clear();
  flushed_save_points_.reset(nullptr);
  unflushed_save_points_.reset(nullptr);
  recovered_txn_ = false;
  largest_validated_seq_ = 0;
  assert(active_iterators_.empty());
  active_iterators_.clear();
  untracked_keys_.clear();
}

Status WriteUnpreparedTxn::HandleWrite(std::function<Status()> do_write) {
  Status s;
  if (active_iterators_.empty()) {
    s = MaybeFlushWriteBatchToDB();
    if (!s.ok()) {
      return s;
    }
  }
  s = do_write();
  if (s.ok()) {
    if (snapshot_) {
      largest_validated_seq_ =
          std::max(largest_validated_seq_, snapshot_->GetSequenceNumber());
    } else {
      // TODO(lth): We should use the same number as tracked_at_seq in TryLock,
      // because what is actually being tracked is the sequence number at which
      // this key was locked at.
      largest_validated_seq_ = db_impl_->GetLastPublishedSequence();
    }
  }
  return s;
}

Status WriteUnpreparedTxn::Put(ColumnFamilyHandle* column_family,
                               const Slice& key, const Slice& value,
                               const bool assume_tracked) {
  return HandleWrite([&]() {
    return TransactionBaseImpl::Put(column_family, key, value, assume_tracked);
  });
}

Status WriteUnpreparedTxn::Put(ColumnFamilyHandle* column_family,
                               const SliceParts& key, const SliceParts& value,
                               const bool assume_tracked) {
  return HandleWrite([&]() {
    return TransactionBaseImpl::Put(column_family, key, value, assume_tracked);
  });
}

Status WriteUnpreparedTxn::Merge(ColumnFamilyHandle* column_family,
                                 const Slice& key, const Slice& value,
                                 const bool assume_tracked) {
  return HandleWrite([&]() {
    return TransactionBaseImpl::Merge(column_family, key, value,
                                      assume_tracked);
  });
}

Status WriteUnpreparedTxn::Delete(ColumnFamilyHandle* column_family,
                                  const Slice& key, const bool assume_tracked) {
  return HandleWrite([&]() {
    return TransactionBaseImpl::Delete(column_family, key, assume_tracked);
  });
}

Status WriteUnpreparedTxn::Delete(ColumnFamilyHandle* column_family,
                                  const SliceParts& key,
                                  const bool assume_tracked) {
  return HandleWrite([&]() {
    return TransactionBaseImpl::Delete(column_family, key, assume_tracked);
  });
}

Status WriteUnpreparedTxn::SingleDelete(ColumnFamilyHandle* column_family,
                                        const Slice& key,
                                        const bool assume_tracked) {
  return HandleWrite([&]() {
    return TransactionBaseImpl::SingleDelete(column_family, key,
                                             assume_tracked);
  });
}

Status WriteUnpreparedTxn::SingleDelete(ColumnFamilyHandle* column_family,
                                        const SliceParts& key,
                                        const bool assume_tracked) {
  return HandleWrite([&]() {
    return TransactionBaseImpl::SingleDelete(column_family, key,
                                             assume_tracked);
  });
}

// WriteUnpreparedTxn::RebuildFromWriteBatch is only called on recovery. For
// WriteUnprepared, the write batches have already been written into the
// database during WAL replay, so all we have to do is just to "retrack" the key
// so that rollbacks are possible.
//
// Calling TryLock instead of TrackKey is also possible, but as an optimization,
// recovered transactions do not hold locks on their keys. This follows the
// implementation in PessimisticTransactionDB::Initialize where we set
// skip_concurrency_control to true.
Status WriteUnpreparedTxn::RebuildFromWriteBatch(WriteBatch* wb) {
  struct TrackKeyHandler : public WriteBatch::Handler {
    WriteUnpreparedTxn* txn_;
    bool rollback_merge_operands_;

    TrackKeyHandler(WriteUnpreparedTxn* txn, bool rollback_merge_operands)
        : txn_(txn), rollback_merge_operands_(rollback_merge_operands) {}

    Status PutCF(uint32_t cf, const Slice& key, const Slice&) override {
      txn_->TrackKey(cf, key.ToString(), kMaxSequenceNumber,
                     false /* read_only */, true /* exclusive */);
      return Status::OK();
    }

    Status DeleteCF(uint32_t cf, const Slice& key) override {
      txn_->TrackKey(cf, key.ToString(), kMaxSequenceNumber,
                     false /* read_only */, true /* exclusive */);
      return Status::OK();
    }

    Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
      txn_->TrackKey(cf, key.ToString(), kMaxSequenceNumber,
                     false /* read_only */, true /* exclusive */);
      return Status::OK();
    }

    Status MergeCF(uint32_t cf, const Slice& key, const Slice&) override {
      if (rollback_merge_operands_) {
        txn_->TrackKey(cf, key.ToString(), kMaxSequenceNumber,
                       false /* read_only */, true /* exclusive */);
      }
      return Status::OK();
    }

    // Recovered batches do not contain 2PC markers.
    Status MarkBeginPrepare(bool) override { return Status::InvalidArgument(); }

    Status MarkEndPrepare(const Slice&) override {
      return Status::InvalidArgument();
    }

    Status MarkNoop(bool) override { return Status::InvalidArgument(); }

    Status MarkCommit(const Slice&) override {
      return Status::InvalidArgument();
    }

    Status MarkRollback(const Slice&) override {
      return Status::InvalidArgument();
    }
  };

  TrackKeyHandler handler(this,
                          wupt_db_->txn_db_options_.rollback_merge_operands);
  return wb->Iterate(&handler);
}

Status WriteUnpreparedTxn::MaybeFlushWriteBatchToDB() {
  const bool kPrepared = true;
  Status s;
  if (write_batch_flush_threshold_ > 0 &&
      write_batch_.GetWriteBatch()->Count() > 0 &&
      write_batch_.GetDataSize() >
          static_cast<size_t>(write_batch_flush_threshold_)) {
    assert(GetState() != PREPARED);
    s = FlushWriteBatchToDB(!kPrepared);
  }
  return s;
}

Status WriteUnpreparedTxn::FlushWriteBatchToDB(bool prepared) {
  // If the current write batch contains savepoints, then some special handling
  // is required so that RollbackToSavepoint can work.
  //
  // RollbackToSavepoint is not supported after Prepare() is called, so only do
  // this for unprepared batches.
  if (!prepared && unflushed_save_points_ != nullptr &&
      !unflushed_save_points_->empty()) {
    return FlushWriteBatchWithSavePointToDB();
  }

  return FlushWriteBatchToDBInternal(prepared);
}

Status WriteUnpreparedTxn::FlushWriteBatchToDBInternal(bool prepared) {
  if (name_.empty()) {
    assert(!prepared);
#ifndef NDEBUG
    static std::atomic_ullong autogen_id{0};
    // To avoid changing all tests to call SetName, just autogenerate one.
    if (wupt_db_->txn_db_options_.autogenerate_name) {
      auto s = SetName(std::string("autoxid") +
                       std::to_string(autogen_id.fetch_add(1)));
      assert(s.ok());
    } else
#endif
    {
      return Status::InvalidArgument("Cannot write to DB without SetName.");
    }
  }

  struct UntrackedKeyHandler : public WriteBatch::Handler {
    WriteUnpreparedTxn* txn_;
    bool rollback_merge_operands_;

    UntrackedKeyHandler(WriteUnpreparedTxn* txn, bool rollback_merge_operands)
        : txn_(txn), rollback_merge_operands_(rollback_merge_operands) {}

    Status AddUntrackedKey(uint32_t cf, const Slice& key) {
      auto str = key.ToString();
      PointLockStatus lock_status =
          txn_->tracked_locks_->GetPointLockStatus(cf, str);
      if (!lock_status.locked) {
        txn_->untracked_keys_[cf].push_back(str);
      }
      return Status::OK();
    }

    Status PutCF(uint32_t cf, const Slice& key, const Slice&) override {
      return AddUntrackedKey(cf, key);
    }

    Status DeleteCF(uint32_t cf, const Slice& key) override {
      return AddUntrackedKey(cf, key);
    }

    Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
      return AddUntrackedKey(cf, key);
    }

    Status MergeCF(uint32_t cf, const Slice& key, const Slice&) override {
      if (rollback_merge_operands_) {
        return AddUntrackedKey(cf, key);
      }
      return Status::OK();
    }

    // The only expected 2PC marker is the initial Noop marker.
    Status MarkNoop(bool empty_batch) override {
      return empty_batch ? Status::OK() : Status::InvalidArgument();
    }

    Status MarkBeginPrepare(bool) override { return Status::InvalidArgument(); }

    Status MarkEndPrepare(const Slice&) override {
      return Status::InvalidArgument();
    }

    Status MarkCommit(const Slice&) override {
      return Status::InvalidArgument();
    }

    Status MarkRollback(const Slice&) override {
      return Status::InvalidArgument();
    }
  };

  UntrackedKeyHandler handler(
      this, wupt_db_->txn_db_options_.rollback_merge_operands);
  auto s = GetWriteBatch()->GetWriteBatch()->Iterate(&handler);
  assert(s.ok());

  // TODO(lth): Reduce duplicate code with WritePrepared prepare logic.
  WriteOptions write_options = write_options_;
  write_options.disableWAL = false;
  const bool WRITE_AFTER_COMMIT = true;
  const bool first_prepare_batch = log_number_ == 0;
  // MarkEndPrepare will change Noop marker to the appropriate marker.
  s = WriteBatchInternal::MarkEndPrepare(GetWriteBatch()->GetWriteBatch(),
                                         name_, !WRITE_AFTER_COMMIT, !prepared);
  assert(s.ok());
  // For each duplicate key we account for a new sub-batch
  prepare_batch_cnt_ = GetWriteBatch()->SubBatchCnt();
  // AddPrepared better to be called in the pre-release callback otherwise there
  // is a non-zero chance of max advancing prepare_seq and readers assume the
  // data as committed.
  // Also having it in the PreReleaseCallback allows in-order addition of
  // prepared entries to PreparedHeap and hence enables an optimization. Refer
  // to SmallestUnCommittedSeq for more details.
  AddPreparedCallback add_prepared_callback(
      wpt_db_, db_impl_, prepare_batch_cnt_,
      db_impl_->immutable_db_options().two_write_queues, first_prepare_batch);
  const bool DISABLE_MEMTABLE = true;
  uint64_t seq_used = kMaxSequenceNumber;
  // log_number_ should refer to the oldest log containing uncommitted data
  // from the current transaction. This means that if log_number_ is set,
  // WriteImpl should not overwrite that value, so set wal_used to nullptr if
  // log_number_ is already set.
  s = db_impl_->WriteImpl(write_options, GetWriteBatch()->GetWriteBatch(),
                          /*callback*/ nullptr, /*user_write_cb=*/nullptr,
                          &last_log_number_,
                          /*log ref*/ 0, !DISABLE_MEMTABLE, &seq_used,
                          prepare_batch_cnt_, &add_prepared_callback);
  if (log_number_ == 0) {
    log_number_ = last_log_number_;
  }
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  auto prepare_seq = seq_used;

  // Only call SetId if it hasn't been set yet.
  if (GetId() == 0) {
    SetId(prepare_seq);
  }
  // unprep_seqs_ will also contain prepared seqnos since they are treated in
  // the same way in the prepare/commit callbacks. See the comment on the
  // definition of unprep_seqs_.
  if (s.ok()) {
    unprep_seqs_[prepare_seq] = prepare_batch_cnt_;
  }

  // Reset transaction state.
  if (!prepared) {
    prepare_batch_cnt_ = 0;
    const bool kClear = true;
    TransactionBaseImpl::InitWriteBatch(kClear);
  }

  return s;
}

Status WriteUnpreparedTxn::FlushWriteBatchWithSavePointToDB() {
  assert(unflushed_save_points_ != nullptr &&
         unflushed_save_points_->size() > 0);
  assert(save_points_ != nullptr && save_points_->size() > 0);
  assert(save_points_->size() >= unflushed_save_points_->size());

  // Handler class for creating an unprepared batch from a savepoint.
  struct SavePointBatchHandler : public WriteBatch::Handler {
    WriteBatchWithIndex* wb_;
    const std::map<uint32_t, ColumnFamilyHandle*>& handles_;

    SavePointBatchHandler(
        WriteBatchWithIndex* wb,
        const std::map<uint32_t, ColumnFamilyHandle*>& handles)
        : wb_(wb), handles_(handles) {}

    Status PutCF(uint32_t cf, const Slice& key, const Slice& value) override {
      return wb_->Put(handles_.at(cf), key, value);
    }

    Status DeleteCF(uint32_t cf, const Slice& key) override {
      return wb_->Delete(handles_.at(cf), key);
    }

    Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
      return wb_->SingleDelete(handles_.at(cf), key);
    }

    Status MergeCF(uint32_t cf, const Slice& key, const Slice& value) override {
      return wb_->Merge(handles_.at(cf), key, value);
    }

    // The only expected 2PC marker is the initial Noop marker.
    Status MarkNoop(bool empty_batch) override {
      return empty_batch ? Status::OK() : Status::InvalidArgument();
    }

    Status MarkBeginPrepare(bool) override { return Status::InvalidArgument(); }

    Status MarkEndPrepare(const Slice&) override {
      return Status::InvalidArgument();
    }

    Status MarkCommit(const Slice&) override {
      return Status::InvalidArgument();
    }

    Status MarkRollback(const Slice&) override {
      return Status::InvalidArgument();
    }
  };

  // The comparator of the default cf is passed in, similar to the
  // initialization of TransactionBaseImpl::write_batch_. This comparator is
  // only used if the write batch encounters an invalid cf id, and falls back to
  // this comparator.
  WriteBatchWithIndex wb(wpt_db_->DefaultColumnFamily()->GetComparator(), 0,
                         true, 0, write_options_.protection_bytes_per_key);
  // Swap with write_batch_ so that wb contains the complete write batch. The
  // actual write batch that will be flushed to DB will be built in
  // write_batch_, and will be read by FlushWriteBatchToDBInternal.
  std::swap(wb, write_batch_);
  TransactionBaseImpl::InitWriteBatch();

  size_t prev_boundary = WriteBatchInternal::kHeader;
  const bool kPrepared = true;
  for (size_t i = 0; i < unflushed_save_points_->size() + 1; i++) {
    bool trailing_batch = i == unflushed_save_points_->size();
    SavePointBatchHandler sp_handler(&write_batch_,
                                     *wupt_db_->GetCFHandleMap().get());
    size_t curr_boundary = trailing_batch ? wb.GetWriteBatch()->GetDataSize()
                                          : (*unflushed_save_points_)[i];

    // Construct the partial write batch up to the savepoint.
    //
    // Theoretically, a memcpy between the write batches should be sufficient
    // since the rewriting into the batch should produce the exact same byte
    // representation. Rebuilding the WriteBatchWithIndex index is still
    // necessary though, and would imply doing two passes over the batch though.
    Status s = WriteBatchInternal::Iterate(wb.GetWriteBatch(), &sp_handler,
                                           prev_boundary, curr_boundary);
    if (!s.ok()) {
      return s;
    }

    if (write_batch_.GetWriteBatch()->Count() > 0) {
      // Flush the write batch.
      s = FlushWriteBatchToDBInternal(!kPrepared);
      if (!s.ok()) {
        return s;
      }
    }

    if (!trailing_batch) {
      if (flushed_save_points_ == nullptr) {
        flushed_save_points_.reset(
            new autovector<WriteUnpreparedTxn::SavePoint>());
      }
      flushed_save_points_->emplace_back(
          unprep_seqs_, new ManagedSnapshot(db_impl_, wupt_db_->GetSnapshot()));
    }

    prev_boundary = curr_boundary;
    const bool kClear = true;
    TransactionBaseImpl::InitWriteBatch(kClear);
  }

  unflushed_save_points_->clear();
  return Status::OK();
}

Status WriteUnpreparedTxn::PrepareInternal() {
  const bool kPrepared = true;
  return FlushWriteBatchToDB(kPrepared);
}

Status WriteUnpreparedTxn::CommitWithoutPrepareInternal() {
  if (unprep_seqs_.empty()) {
    assert(log_number_ == 0);
    assert(GetId() == 0);
    return WritePreparedTxn::CommitWithoutPrepareInternal();
  }

  // TODO(lth): We should optimize commit without prepare to not perform
  // a prepare under the hood.
  auto s = PrepareInternal();
  if (!s.ok()) {
    return s;
  }
  return CommitInternal();
}

Status WriteUnpreparedTxn::CommitInternal() {
  // TODO(lth): Reduce duplicate code with WritePrepared commit logic.

  // We take the commit-time batch and append the Commit marker.  The Memtable
  // will ignore the Commit marker in non-recovery mode
  WriteBatch* working_batch = GetCommitTimeWriteBatch();
  const bool empty = working_batch->Count() == 0;
  auto s = WriteBatchInternal::MarkCommit(working_batch, name_);
  assert(s.ok());

  const bool for_recovery = use_only_the_last_commit_time_batch_for_recovery_;
  if (!empty) {
    // When not writing to memtable, we can still cache the latest write batch.
    // The cached batch will be written to memtable in WriteRecoverableState
    // during FlushMemTable
    if (for_recovery) {
      WriteBatchInternal::SetAsLatestPersistentState(working_batch);
    } else {
      return Status::InvalidArgument(
          "Commit-time-batch can only be used if "
          "use_only_the_last_commit_time_batch_for_recovery is true");
    }
  }

  const bool includes_data = !empty && !for_recovery;
  size_t commit_batch_cnt = 0;
  if (UNLIKELY(includes_data)) {
    ROCKS_LOG_WARN(db_impl_->immutable_db_options().info_log,
                   "Duplicate key overhead");
    SubBatchCounter counter(*wpt_db_->GetCFComparatorMap());
    s = working_batch->Iterate(&counter);
    assert(s.ok());
    commit_batch_cnt = counter.BatchCount();
  }
  const bool disable_memtable = !includes_data;
  const bool do_one_write =
      !db_impl_->immutable_db_options().two_write_queues || disable_memtable;

  WriteUnpreparedCommitEntryPreReleaseCallback update_commit_map(
      wpt_db_, db_impl_, unprep_seqs_, commit_batch_cnt);
  const bool kFirstPrepareBatch = true;
  AddPreparedCallback add_prepared_callback(
      wpt_db_, db_impl_, commit_batch_cnt,
      db_impl_->immutable_db_options().two_write_queues, !kFirstPrepareBatch);
  PreReleaseCallback* pre_release_callback;
  if (do_one_write) {
    pre_release_callback = &update_commit_map;
  } else {
    pre_release_callback = &add_prepared_callback;
  }
  uint64_t seq_used = kMaxSequenceNumber;
  // Since the prepared batch is directly written to memtable, there is
  // already a connection between the memtable and its WAL, so there is no
  // need to redundantly reference the log that contains the prepared data.
  const uint64_t zero_log_number = 0ull;
  size_t batch_cnt = UNLIKELY(commit_batch_cnt) ? commit_batch_cnt : 1;
  s = db_impl_->WriteImpl(write_options_, working_batch, nullptr, nullptr,
                          nullptr, zero_log_number, disable_memtable, &seq_used,
                          batch_cnt, pre_release_callback);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  const SequenceNumber commit_batch_seq = seq_used;
  if (LIKELY(do_one_write || !s.ok())) {
    if (LIKELY(s.ok())) {
      // Note RemovePrepared should be called after WriteImpl that publishsed
      // the seq. Otherwise SmallestUnCommittedSeq optimization breaks.
      for (const auto& seq : unprep_seqs_) {
        wpt_db_->RemovePrepared(seq.first, seq.second);
      }
      unprep_seqs_.clear();
      flushed_save_points_.reset(nullptr);
      unflushed_save_points_.reset(nullptr);
    } else if (UNLIKELY(!do_one_write)) {
      // The commit write failed after add_prepared_callback may have added a
      // prepared entry for commit_batch_seq. Since commit_batch_seq is never
      // stored in unprep_seqs_ on this path, a follow-up Rollback() cannot
      // remove it, so clean it up here. unprep_seqs_ is otherwise preserved so
      // the transaction can still be rolled back after the caller recovers
      // from the write error.
      wpt_db_->RemovePrepared(commit_batch_seq, commit_batch_cnt);
    }
    return s;
  }  // else do the 2nd write to publish seq

  // Populate unprep_seqs_ with commit_batch_seq, since we treat data in the
  // commit write batch as just another "unprepared" batch. This will also
  // update the unprep_seqs_ in the update_commit_map callback.
  unprep_seqs_[commit_batch_seq] = commit_batch_cnt;
  WriteUnpreparedCommitEntryPreReleaseCallback
      update_commit_map_with_commit_batch(wpt_db_, db_impl_, unprep_seqs_, 0);

  // Note: the 2nd write comes with a performance penality. So if we have too
  // many of commits accompanied with ComitTimeWriteBatch and yet we cannot
  // enable use_only_the_last_commit_time_batch_for_recovery_ optimization,
  // two_write_queues should be disabled to avoid many additional writes here.

  // Update commit map only from the 2nd queue
  WriteBatch empty_batch;
  s = empty_batch.PutLogData(Slice());
  assert(s.ok());
  // In the absence of Prepare markers, use Noop as a batch separator
  s = WriteBatchInternal::InsertNoop(&empty_batch);
  assert(s.ok());
  const bool DISABLE_MEMTABLE = true;
  const size_t ONE_BATCH = 1;
  const uint64_t NO_REF_LOG = 0;
  s = db_impl_->WriteImpl(write_options_, &empty_batch, nullptr, nullptr,
                          nullptr, NO_REF_LOG, DISABLE_MEMTABLE, &seq_used,
                          ONE_BATCH, &update_commit_map_with_commit_batch);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  // Note RemovePrepared should be called after WriteImpl that publishsed the
  // seq. Otherwise SmallestUnCommittedSeq optimization breaks.
  for (const auto& seq : unprep_seqs_) {
    wpt_db_->RemovePrepared(seq.first, seq.second);
  }
  unprep_seqs_.clear();
  flushed_save_points_.reset(nullptr);
  unflushed_save_points_.reset(nullptr);
  return s;
}

Status WriteUnpreparedTxn::WriteRollbackKeys(
    const LockTracker& lock_tracker, WriteBatchWithIndex* rollback_batch,
    ReadCallback* callback, const ReadOptions& roptions) {
  // This assertion can be removed when range lock is supported.
  assert(lock_tracker.IsPointLockSupported());
  const auto& cf_map = *wupt_db_->GetCFHandleMap();
  auto WriteRollbackKey = [&](const std::string& key, uint32_t cfid) {
    const auto& cf_handle = cf_map.at(cfid);
    PinnableSlice pinnable_val;
    bool not_used;
    DBImpl::GetImplOptions get_impl_options;
    get_impl_options.column_family = cf_handle;
    get_impl_options.value = &pinnable_val;
    get_impl_options.value_found = &not_used;
    get_impl_options.callback = callback;
    auto s = db_impl_->GetImpl(roptions, key, get_impl_options);

    if (s.ok()) {
      s = rollback_batch->Put(cf_handle, key, pinnable_val);
      assert(s.ok());
    } else if (s.IsNotFound()) {
      if (wupt_db_->ShouldRollbackWithSingleDelete(cf_handle, key)) {
        s = rollback_batch->SingleDelete(cf_handle, key);
      } else {
        s = rollback_batch->Delete(cf_handle, key);
      }
      assert(s.ok());
    } else {
      return s;
    }

    return Status::OK();
  };

  std::unique_ptr<LockTracker::ColumnFamilyIterator> cf_it(
      lock_tracker.GetColumnFamilyIterator());
  assert(cf_it != nullptr);
  while (cf_it->HasNext()) {
    ColumnFamilyId cf = cf_it->Next();
    std::unique_ptr<LockTracker::KeyIterator> key_it(
        lock_tracker.GetKeyIterator(cf));
    assert(key_it != nullptr);
    while (key_it->HasNext()) {
      const std::string& key = key_it->Next();
      auto s = WriteRollbackKey(key, cf);
      if (!s.ok()) {
        return s;
      }
    }
  }

  for (const auto& cfkey : untracked_keys_) {
    const auto cfid = cfkey.first;
    const auto& keys = cfkey.second;
    for (const auto& key : keys) {
      auto s = WriteRollbackKey(key, cfid);
      if (!s.ok()) {
        return s;
      }
    }
  }

  return Status::OK();
}

Status WriteUnpreparedTxn::RollbackInternal() {
  // TODO(lth): Reduce duplicate code with WritePrepared rollback logic.
  WriteBatchWithIndex rollback_batch(
      wpt_db_->DefaultColumnFamily()->GetComparator(), 0, true, 0,
      write_options_.protection_bytes_per_key);
  assert(GetId() != kMaxSequenceNumber);
  assert(GetId() > 0);
  Status s;
  auto read_at_seq = kMaxSequenceNumber;
  // TODO: plumb Env::IOActivity, Env::IOPriority
  ReadOptions roptions;
  // to prevent callback's seq to be overrriden inside DBImpk::Get
  roptions.snapshot = wpt_db_->GetMaxSnapshot();
  // Note that we do not use WriteUnpreparedTxnReadCallback because we do not
  // need to read our own writes when reading prior versions of the key for
  // rollback.
  WritePreparedTxnReadCallback callback(wpt_db_, read_at_seq);
  // TODO(lth): We write rollback batch all in a single batch here, but this
  // should be subdivded into multiple batches as well. In phase 2, when key
  // sets are read from WAL, this will happen naturally.
  s = WriteRollbackKeys(*tracked_locks_, &rollback_batch, &callback, roptions);
  if (!s.ok()) {
    return s;
  }

  // The Rollback marker will be used as a batch separator
  s = WriteBatchInternal::MarkRollback(rollback_batch.GetWriteBatch(), name_);
  assert(s.ok());
  bool do_one_write = !db_impl_->immutable_db_options().two_write_queues;
  const bool DISABLE_MEMTABLE = true;
  const uint64_t NO_REF_LOG = 0;
  uint64_t seq_used = kMaxSequenceNumber;
  // Rollback batch may contain duplicate keys, because tracked_keys_ is not
  // comparator aware.
  auto rollback_batch_cnt = rollback_batch.SubBatchCnt();
  // We commit the rolled back prepared batches. Although this is
  // counter-intuitive, i) it is safe to do so, since the prepared batches are
  // already canceled out by the rollback batch, ii) adding the commit entry to
  // CommitCache will allow us to benefit from the existing mechanism in
  // CommitCache that keeps an entry evicted due to max advance and yet overlaps
  // with a live snapshot around so that the live snapshot properly skips the
  // entry even if its prepare seq is lower than max_evicted_seq_.
  //
  // TODO(lth): RollbackInternal is conceptually very similar to
  // CommitInternal, with the rollback batch simply taking on the role of
  // CommitTimeWriteBatch. We should be able to merge the two code paths.
  WriteUnpreparedCommitEntryPreReleaseCallback update_commit_map(
      wpt_db_, db_impl_, unprep_seqs_, rollback_batch_cnt);
  // Note: the rollback batch does not need AddPrepared since it is written to
  // DB in one shot. min_uncommitted still works since it requires capturing
  // data that is written to DB but not yet committed, while the rollback
  // batch commits with PreReleaseCallback.
  s = db_impl_->WriteImpl(write_options_, rollback_batch.GetWriteBatch(),
                          nullptr, nullptr, nullptr, NO_REF_LOG,
                          !DISABLE_MEMTABLE, &seq_used, rollback_batch_cnt,
                          do_one_write ? &update_commit_map : nullptr);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  if (!s.ok()) {
    return s;
  }
  if (do_one_write) {
    for (const auto& seq : unprep_seqs_) {
      wpt_db_->RemovePrepared(seq.first, seq.second);
    }
    unprep_seqs_.clear();
    flushed_save_points_.reset(nullptr);
    unflushed_save_points_.reset(nullptr);
    return s;
  }  // else do the 2nd write for commit

  uint64_t& prepare_seq = seq_used;
  // Populate unprep_seqs_ with rollback_batch_cnt, since we treat data in the
  // rollback write batch as just another "unprepared" batch. This will also
  // update the unprep_seqs_ in the update_commit_map callback.
  unprep_seqs_[prepare_seq] = rollback_batch_cnt;
  WriteUnpreparedCommitEntryPreReleaseCallback
      update_commit_map_with_rollback_batch(wpt_db_, db_impl_, unprep_seqs_, 0);

  ROCKS_LOG_DETAILS(db_impl_->immutable_db_options().info_log,
                    "RollbackInternal 2nd write prepare_seq: %" PRIu64,
                    prepare_seq);
  WriteBatch empty_batch;
  const size_t ONE_BATCH = 1;
  s = empty_batch.PutLogData(Slice());
  assert(s.ok());
  // In the absence of Prepare markers, use Noop as a batch separator
  s = WriteBatchInternal::InsertNoop(&empty_batch);
  assert(s.ok());
  s = db_impl_->WriteImpl(write_options_, &empty_batch, nullptr, nullptr,
                          nullptr, NO_REF_LOG, DISABLE_MEMTABLE, &seq_used,
                          ONE_BATCH, &update_commit_map_with_rollback_batch);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  // Mark the txn as rolled back
  if (s.ok()) {
    for (const auto& seq : unprep_seqs_) {
      wpt_db_->RemovePrepared(seq.first, seq.second);
    }
  }

  unprep_seqs_.clear();
  flushed_save_points_.reset(nullptr);
  unflushed_save_points_.reset(nullptr);
  return s;
}

void WriteUnpreparedTxn::Clear() {
  if (!recovered_txn_) {
    txn_db_impl_->UnLock(this, *tracked_locks_);
  }
  unprep_seqs_.clear();
  flushed_save_points_.reset(nullptr);
  unflushed_save_points_.reset(nullptr);
  recovered_txn_ = false;
  largest_validated_seq_ = 0;
  for (auto& it : active_iterators_) {
    auto bdit = static_cast<BaseDeltaIterator*>(it);
    bdit->Invalidate(Status::InvalidArgument(
        "Cannot use iterator after transaction has finished"));
  }
  active_iterators_.clear();
  untracked_keys_.clear();
  TransactionBaseImpl::Clear();
}

void WriteUnpreparedTxn::SetSavePoint() {
  assert((unflushed_save_points_ ? unflushed_save_points_->size() : 0) +
             (flushed_save_points_ ? flushed_save_points_->size() : 0) ==
         (save_points_ ? save_points_->size() : 0));
  PessimisticTransaction::SetSavePoint();
  if (unflushed_save_points_ == nullptr) {
    unflushed_save_points_.reset(new autovector<size_t>());
  }
  unflushed_save_points_->push_back(write_batch_.GetDataSize());
}

Status WriteUnpreparedTxn::RollbackToSavePoint() {
  assert((unflushed_save_points_ ? unflushed_save_points_->size() : 0) +
             (flushed_save_points_ ? flushed_save_points_->size() : 0) ==
         (save_points_ ? save_points_->size() : 0));
  if (unflushed_save_points_ != nullptr && unflushed_save_points_->size() > 0) {
    Status s = PessimisticTransaction::RollbackToSavePoint();
    assert(!s.IsNotFound());
    unflushed_save_points_->pop_back();
    return s;
  }

  if (flushed_save_points_ != nullptr && !flushed_save_points_->empty()) {
    return RollbackToSavePointInternal();
  }

  return Status::NotFound();
}

Status WriteUnpreparedTxn::RollbackToSavePointInternal() {
  Status s;

  const bool kClear = true;
  TransactionBaseImpl::InitWriteBatch(kClear);

  assert(flushed_save_points_->size() > 0);
  WriteUnpreparedTxn::SavePoint& top = flushed_save_points_->back();

  assert(save_points_ != nullptr && save_points_->size() > 0);
  const LockTracker& tracked_keys = *save_points_->top().new_locks_;

  // TODO: plumb Env::IOActivity, Env::IOPriority
  ReadOptions roptions;
  roptions.snapshot = top.snapshot_->snapshot();
  SequenceNumber min_uncommitted =
      static_cast_with_check<const SnapshotImpl>(roptions.snapshot)
          ->min_uncommitted_;
  SequenceNumber snap_seq = roptions.snapshot->GetSequenceNumber();
  WriteUnpreparedTxnReadCallback callback(wupt_db_, snap_seq, min_uncommitted,
                                          top.unprep_seqs_,
                                          kBackedByDBSnapshot);
  s = WriteRollbackKeys(tracked_keys, &write_batch_, &callback, roptions);
  if (!s.ok()) {
    return s;
  }

  const bool kPrepared = true;
  s = FlushWriteBatchToDBInternal(!kPrepared);
  if (!s.ok()) {
    return s;
  }

  // PessimisticTransaction::RollbackToSavePoint will call also call
  // RollbackToSavepoint on write_batch_. However, write_batch_ is empty and has
  // no savepoints because this savepoint has already been flushed. Work around
  // this by setting a fake savepoint.
  write_batch_.SetSavePoint();
  s = PessimisticTransaction::RollbackToSavePoint();
  assert(s.ok());
  if (!s.ok()) {
    return s;
  }

  flushed_save_points_->pop_back();
  return s;
}

Status WriteUnpreparedTxn::PopSavePoint() {
  assert((unflushed_save_points_ ? unflushed_save_points_->size() : 0) +
             (flushed_save_points_ ? flushed_save_points_->size() : 0) ==
         (save_points_ ? save_points_->size() : 0));
  if (unflushed_save_points_ != nullptr && unflushed_save_points_->size() > 0) {
    Status s = PessimisticTransaction::PopSavePoint();
    assert(!s.IsNotFound());
    unflushed_save_points_->pop_back();
    return s;
  }

  if (flushed_save_points_ != nullptr && !flushed_save_points_->empty()) {
    // PessimisticTransaction::PopSavePoint will call also call PopSavePoint on
    // write_batch_. However, write_batch_ is empty and has no savepoints
    // because this savepoint has already been flushed. Work around this by
    // setting a fake savepoint.
    write_batch_.SetSavePoint();
    Status s = PessimisticTransaction::PopSavePoint();
    assert(!s.IsNotFound());
    flushed_save_points_->pop_back();
    return s;
  }

  return Status::NotFound();
}

void WriteUnpreparedTxn::MultiGetImpl(
    const ReadOptions& read_options, ColumnFamilyHandle* column_family,
    size_t num_keys, const Slice* keys, PinnableSlice* values, Status* statuses,
    bool sorted_input) {
  SequenceNumber min_uncommitted;
  SequenceNumber snap_seq;
  const SnapshotBackup backed_by_snapshot = wupt_db_->AssignMinMaxSeqs(
      read_options.snapshot, &min_uncommitted, &snap_seq);
  WriteUnpreparedTxnReadCallback callback(wupt_db_, snap_seq, min_uncommitted,
                                          unprep_seqs_, backed_by_snapshot);
  write_batch_.MultiGetFromBatchAndDB(db_, read_options, column_family,
                                      num_keys, keys, values, statuses,
                                      sorted_input, &callback);
  if (UNLIKELY(!callback.valid() ||
               !wupt_db_->ValidateSnapshot(snap_seq, backed_by_snapshot))) {
    wupt_db_->WPRecordTick(TXN_GET_TRY_AGAIN);
    for (size_t i = 0; i < num_keys; i++) {
      statuses[i] = Status::TryAgain();
    }
  }
}

Status WriteUnpreparedTxn::Get(const ReadOptions& _read_options,
                               ColumnFamilyHandle* column_family,
                               const Slice& key, PinnableSlice* value) {
  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kGet) {
    return Status::InvalidArgument(
        "Can only call Get with `ReadOptions::io_activity` is "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kGet`");
  }
  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kGet;
  }

  return GetImpl(read_options, column_family, key, value);
}

Status WriteUnpreparedTxn::GetImpl(const ReadOptions& options,
                                   ColumnFamilyHandle* column_family,
                                   const Slice& key, PinnableSlice* value) {
  SequenceNumber min_uncommitted;
  SequenceNumber snap_seq;
  const SnapshotBackup backed_by_snapshot =
      wupt_db_->AssignMinMaxSeqs(options.snapshot, &min_uncommitted, &snap_seq);
  WriteUnpreparedTxnReadCallback callback(wupt_db_, snap_seq, min_uncommitted,
                                          unprep_seqs_, backed_by_snapshot);
  auto res = write_batch_.GetFromBatchAndDB(db_, options, column_family, key,
                                            value, &callback);
  if (LIKELY(callback.valid() &&
             wupt_db_->ValidateSnapshot(snap_seq, backed_by_snapshot))) {
    return res;
  } else {
    res.PermitUncheckedError();
    wupt_db_->WPRecordTick(TXN_GET_TRY_AGAIN);
    return Status::TryAgain();
  }
}

Status WriteUnpreparedTxn::GetEntityImpl(const ReadOptions& options,
                                         ColumnFamilyHandle* column_family,
                                         const Slice& key,
                                         PinnableWideColumns* columns) {
  SequenceNumber min_uncommitted;
  SequenceNumber snap_seq;
  const SnapshotBackup backed_by_snapshot =
      wupt_db_->AssignMinMaxSeqs(options.snapshot, &min_uncommitted, &snap_seq);
  WriteUnpreparedTxnReadCallback callback(wupt_db_, snap_seq, min_uncommitted,
                                          unprep_seqs_, backed_by_snapshot);
  Status res = write_batch_.GetEntityFromBatchAndDB(db_, options, column_family,
                                                    key, columns, &callback);
  if (LIKELY(callback.valid() &&
             wupt_db_->ValidateSnapshot(snap_seq, backed_by_snapshot))) {
    return res;
  } else {
    res.PermitUncheckedError();
    wupt_db_->WPRecordTick(TXN_GET_TRY_AGAIN);
    return Status::TryAgain();
  }
}

void WriteUnpreparedTxn::MultiGetEntityImpl(const ReadOptions& options,
                                            ColumnFamilyHandle* column_family,
                                            size_t num_keys, const Slice* keys,
                                            PinnableWideColumns* results,
                                            Status* statuses,
                                            bool sorted_input) {
  SequenceNumber min_uncommitted;
  SequenceNumber snap_seq;
  const SnapshotBackup backed_by_snapshot =
      wupt_db_->AssignMinMaxSeqs(options.snapshot, &min_uncommitted, &snap_seq);
  WriteUnpreparedTxnReadCallback callback(wupt_db_, snap_seq, min_uncommitted,
                                          unprep_seqs_, backed_by_snapshot);
  write_batch_.MultiGetEntityFromBatchAndDB(db_, options, column_family,
                                            num_keys, keys, results, statuses,
                                            sorted_input, &callback);
  if (UNLIKELY(!callback.valid() ||
               !wupt_db_->ValidateSnapshot(snap_seq, backed_by_snapshot))) {
    wupt_db_->WPRecordTick(TXN_GET_TRY_AGAIN);
    for (size_t i = 0; i < num_keys; i++) {
      statuses[i] = Status::TryAgain();
    }
  }
}

namespace {
static void CleanupWriteUnpreparedWBWIIterator(void* arg1, void* arg2) {
  auto txn = static_cast<WriteUnpreparedTxn*>(arg1);
  auto iter = static_cast<Iterator*>(arg2);
  txn->RemoveActiveIterator(iter);
}
}  // anonymous namespace

Iterator* WriteUnpreparedTxn::GetIterator(const ReadOptions& options) {
  return GetIterator(options, wupt_db_->DefaultColumnFamily());
}

Iterator* WriteUnpreparedTxn::GetIterator(const ReadOptions& options,
                                          ColumnFamilyHandle* column_family) {
  // Make sure to get iterator from WriteUnprepareTxnDB, not the root db.
  Iterator* db_iter = wupt_db_->NewIterator(options, column_family, this);
  assert(db_iter);

  auto iter =
      write_batch_.NewIteratorWithBase(column_family, db_iter, &options);
  active_iterators_.push_back(iter);
  iter->RegisterCleanup(CleanupWriteUnpreparedWBWIIterator, this, iter);
  return iter;
}

Status WriteUnpreparedTxn::ValidateSnapshot(ColumnFamilyHandle* column_family,
                                            const Slice& key,
                                            SequenceNumber* tracked_at_seq) {
  // TODO(lth): Reduce duplicate code with WritePrepared ValidateSnapshot logic.
  assert(snapshot_);

  SequenceNumber min_uncommitted =
      static_cast_with_check<const SnapshotImpl>(snapshot_.get())
          ->min_uncommitted_;
  SequenceNumber snap_seq = snapshot_->GetSequenceNumber();
  // tracked_at_seq is either max or the last snapshot with which this key was
  // trackeed so there is no need to apply the IsInSnapshot to this comparison
  // here as tracked_at_seq is not a prepare seq.
  if (*tracked_at_seq <= snap_seq) {
    // If the key has been previous validated at a sequence number earlier
    // than the curent snapshot's sequence number, we already know it has not
    // been modified.
    return Status::OK();
  }

  *tracked_at_seq = snap_seq;

  ColumnFamilyHandle* cfh =
      column_family ? column_family : db_impl_->DefaultColumnFamily();

  WriteUnpreparedTxnReadCallback snap_checker(
      wupt_db_, snap_seq, min_uncommitted, unprep_seqs_, kBackedByDBSnapshot);
  // TODO(yanqin): Support user-defined timestamp.
  return TransactionUtil::CheckKeyForConflicts(
      db_impl_, cfh, key.ToString(), snap_seq, /*ts=*/nullptr,
      false /* cache_only */, &snap_checker, min_uncommitted,
      txn_db_impl_->GetTxnDBOptions().enable_udt_validation);
}

const std::map<SequenceNumber, size_t>&
WriteUnpreparedTxn::GetUnpreparedSequenceNumbers() {
  return unprep_seqs_;
}

}  // namespace ROCKSDB_NAMESPACE
