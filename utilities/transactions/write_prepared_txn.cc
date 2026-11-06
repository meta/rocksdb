//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/transactions/write_prepared_txn.h"

#include <cinttypes>
#include <map>
#include <set>

#include "db/attribute_group_iterator_impl.h"
#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/transaction_db.h"
#include "util/cast_util.h"
#include "utilities/transactions/pessimistic_transaction.h"
#include "utilities/transactions/write_prepared_txn_db.h"

namespace ROCKSDB_NAMESPACE {

struct WriteOptions;

WritePreparedTxn::WritePreparedTxn(WritePreparedTxnDB* txn_db,
                                   const WriteOptions& write_options,
                                   const TransactionOptions& txn_options)
    : PessimisticTransaction(txn_db, write_options, txn_options, false),
      wpt_db_(txn_db) {
  // Call Initialize outside PessimisticTransaction constructor otherwise it
  // would skip overridden functions in WritePreparedTxn since they are not
  // defined yet in the constructor of PessimisticTransaction
  WritePreparedTxn::Initialize(txn_options);
}

void WritePreparedTxn::Initialize(const TransactionOptions& txn_options) {
  PessimisticTransaction::Initialize(txn_options);
  prepare_batch_cnt_ = 0;
}

void WritePreparedTxn::MultiGetImpl(
    const ReadOptions& read_options, ColumnFamilyHandle* column_family,
    size_t num_keys, const Slice* keys, PinnableSlice* values, Status* statuses,
    bool sorted_input) {
  SequenceNumber min_uncommitted;
  SequenceNumber snap_seq;
  const SnapshotBackup backed_by_snapshot = wpt_db_->AssignMinMaxSeqs(
      read_options.snapshot, &min_uncommitted, &snap_seq);
  WritePreparedTxnReadCallback callback(wpt_db_, snap_seq, min_uncommitted,
                                        backed_by_snapshot);
  write_batch_.MultiGetFromBatchAndDB(db_, read_options, column_family,
                                      num_keys, keys, values, statuses,
                                      sorted_input, &callback);
  if (UNLIKELY(!callback.valid() ||
               !wpt_db_->ValidateSnapshot(snap_seq, backed_by_snapshot))) {
    wpt_db_->WPRecordTick(TXN_GET_TRY_AGAIN);
    for (size_t i = 0; i < num_keys; i++) {
      statuses[i] = Status::TryAgain();
    }
  }
}

Status WritePreparedTxn::Get(const ReadOptions& _read_options,
                             ColumnFamilyHandle* column_family,
                             const Slice& key, PinnableSlice* pinnable_val) {
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

  return GetImpl(read_options, column_family, key, pinnable_val);
}

Status WritePreparedTxn::GetImpl(const ReadOptions& options,
                                 ColumnFamilyHandle* column_family,
                                 const Slice& key,
                                 PinnableSlice* pinnable_val) {
  SequenceNumber min_uncommitted;
  SequenceNumber snap_seq;
  const SnapshotBackup backed_by_snapshot =
      wpt_db_->AssignMinMaxSeqs(options.snapshot, &min_uncommitted, &snap_seq);
  WritePreparedTxnReadCallback callback(wpt_db_, snap_seq, min_uncommitted,
                                        backed_by_snapshot);
  Status res = write_batch_.GetFromBatchAndDB(db_, options, column_family, key,
                                              pinnable_val, &callback);
  const bool callback_valid =
      callback.valid();  // NOTE: validity of callback must always be checked
                         // before it is destructed
  if (res.ok()) {
    if (!LIKELY(callback_valid &&
                wpt_db_->ValidateSnapshot(callback.max_visible_seq(),
                                          backed_by_snapshot))) {
      wpt_db_->WPRecordTick(TXN_GET_TRY_AGAIN);
      res = Status::TryAgain();
    }
  }

  return res;
}

Status WritePreparedTxn::GetEntityImpl(const ReadOptions& options,
                                       ColumnFamilyHandle* column_family,
                                       const Slice& key,
                                       PinnableWideColumns* columns) {
  SequenceNumber min_uncommitted;
  SequenceNumber snap_seq;
  const SnapshotBackup backed_by_snapshot =
      wpt_db_->AssignMinMaxSeqs(options.snapshot, &min_uncommitted, &snap_seq);
  WritePreparedTxnReadCallback callback(wpt_db_, snap_seq, min_uncommitted,
                                        backed_by_snapshot);
  Status res = write_batch_.GetEntityFromBatchAndDB(db_, options, column_family,
                                                    key, columns, &callback);
  const bool callback_valid =
      callback.valid();  // NOTE: validity of callback must always be checked
                         // before it is destructed
  if (res.ok()) {
    if (!LIKELY(callback_valid &&
                wpt_db_->ValidateSnapshot(callback.max_visible_seq(),
                                          backed_by_snapshot))) {
      wpt_db_->WPRecordTick(TXN_GET_TRY_AGAIN);
      res = Status::TryAgain();
    }
  }

  return res;
}

void WritePreparedTxn::MultiGetEntityImpl(const ReadOptions& options,
                                          ColumnFamilyHandle* column_family,
                                          size_t num_keys, const Slice* keys,
                                          PinnableWideColumns* results,
                                          Status* statuses, bool sorted_input) {
  SequenceNumber min_uncommitted;
  SequenceNumber snap_seq;
  const SnapshotBackup backed_by_snapshot =
      wpt_db_->AssignMinMaxSeqs(options.snapshot, &min_uncommitted, &snap_seq);
  WritePreparedTxnReadCallback callback(wpt_db_, snap_seq, min_uncommitted,
                                        backed_by_snapshot);
  write_batch_.MultiGetEntityFromBatchAndDB(db_, options, column_family,
                                            num_keys, keys, results, statuses,
                                            sorted_input, &callback);
  // Without an explicit snapshot, the DB read refreshes the callback to the
  // actual read sequence while snap_seq remains zero.
  if (UNLIKELY(!callback.valid() ||
               !wpt_db_->ValidateSnapshot(callback.max_visible_seq(),
                                          backed_by_snapshot))) {
    wpt_db_->WPRecordTick(TXN_GET_TRY_AGAIN);
    for (size_t i = 0; i < num_keys; i++) {
      statuses[i] = Status::TryAgain();
    }
  }
}

Iterator* WritePreparedTxn::GetIterator(const ReadOptions& options) {
  return GetIterator(options, wpt_db_->DefaultColumnFamily());
}

Iterator* WritePreparedTxn::GetIterator(const ReadOptions& options,
                                        ColumnFamilyHandle* column_family) {
  // Make sure to get iterator from WritePrepareTxnDB, not the root db.
  Iterator* db_iter = wpt_db_->NewIterator(options, column_family);
  assert(db_iter);

  return write_batch_.NewIteratorWithBase(column_family, db_iter, &options);
}

std::unique_ptr<Iterator> WritePreparedTxn::GetCoalescingIterator(
    const ReadOptions& /* read_options */,
    const std::vector<ColumnFamilyHandle*>& /* column_families */) {
  return std::unique_ptr<Iterator>(NewErrorIterator(
      Status::NotSupported("GetCoalescingIterator not supported for "
                           "write-prepared/write-unprepared transactions")));
}

std::unique_ptr<AttributeGroupIterator>
WritePreparedTxn::GetAttributeGroupIterator(
    const ReadOptions& /* read_options */,
    const std::vector<ColumnFamilyHandle*>& /* column_families */) {
  return NewAttributeGroupErrorIterator(
      Status::NotSupported("GetAttributeGroupIterator not supported for "
                           "write-prepared/write-unprepared transactions"));
}

Status WritePreparedTxn::PrepareInternal() {
  WriteOptions write_options = write_options_;
  write_options.disableWAL = false;
  const bool WRITE_AFTER_COMMIT = true;
  const bool kFirstPrepareBatch = true;
  auto s = WriteBatchInternal::MarkEndPrepare(GetWriteBatch()->GetWriteBatch(),
                                              name_, !WRITE_AFTER_COMMIT);
  assert(s.ok());
  // For each duplicate key we account for a new sub-batch
  prepare_batch_cnt_ = GetWriteBatch()->SubBatchCnt();
  // Having AddPrepared in the PreReleaseCallback allows in-order addition of
  // prepared entries to PreparedHeap and hence enables an optimization. Refer
  // to SmallestUnCommittedSeq for more details.
  AddPreparedCallback add_prepared_callback(
      wpt_db_, db_impl_, prepare_batch_cnt_,
      db_impl_->immutable_db_options().two_write_queues, kFirstPrepareBatch);
  const bool DISABLE_MEMTABLE = true;
  uint64_t seq_used = kMaxSequenceNumber;
  s = db_impl_->WriteImpl(write_options, GetWriteBatch()->GetWriteBatch(),
                          /*callback*/ nullptr, /*user_write_cb=*/nullptr,
                          &log_number_, /*log ref*/ 0, !DISABLE_MEMTABLE,
                          &seq_used, prepare_batch_cnt_,
                          &add_prepared_callback);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  auto prepare_seq = seq_used;
  SetId(prepare_seq);
  return s;
}

Status WritePreparedTxn::CommitWithoutPrepareInternal() {
  // For each duplicate key we account for a new sub-batch
  const size_t batch_cnt = GetWriteBatch()->SubBatchCnt();
  return CommitBatchInternal(GetWriteBatch()->GetWriteBatch(), batch_cnt);
}

Status WritePreparedTxn::CommitBatchInternal(WriteBatch* batch,
                                             size_t batch_cnt) {
  return wpt_db_->WriteInternal(write_options_, batch, batch_cnt, this);
}

Status WritePreparedTxn::CommitInternal() {
  ROCKS_LOG_DETAILS(db_impl_->immutable_db_options().info_log,
                    "CommitInternal prepare_seq: %" PRIu64, GetID());
  // We take the commit-time batch and append the Commit marker.
  // The Memtable will ignore the Commit marker in non-recovery mode
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

  auto prepare_seq = GetId();
  const bool includes_data = !empty && !for_recovery;
  assert(prepare_batch_cnt_);
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
  WritePreparedCommitEntryPreReleaseCallback update_commit_map(
      wpt_db_, db_impl_, prepare_seq, prepare_batch_cnt_, commit_batch_cnt);
  // This is to call AddPrepared on CommitTimeWriteBatch
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
  // Since the prepared batch is directly written to memtable, there is already
  // a connection between the memtable and its WAL, so there is no need to
  // redundantly reference the log that contains the prepared data.
  const uint64_t zero_log_number = 0ull;
  size_t batch_cnt = UNLIKELY(commit_batch_cnt) ? commit_batch_cnt : 1;
  // If `two_write_queues && includes_data`, then `do_one_write` is false. The
  // following `WriteImpl` will insert the data of the commit-time-batch into
  // the database before updating the commit cache. Therefore, the data of the
  // commmit-time-batch is considered uncommitted. Furthermore, since data of
  // the commit-time-batch are not locked, it is possible for two uncommitted
  // versions of the same key to co-exist for a (short) period of time until
  // the commit cache is updated by the second write. If the two uncommitted
  // keys are compacted to the bottommost level in the meantime, it is possible
  // that compaction iterator will zero out the sequence numbers of both, thus
  // violating the invariant that an SST does not have two identical internal
  // keys. To prevent this situation, we should allow the usage of
  // commit-time-batch only if the user sets
  // TransactionOptions::use_only_the_last_commit_time_batch_for_recovery to
  // true. See the comments about GetCommitTimeWriteBatch() in
  // include/rocksdb/utilities/transaction.h.
  s = db_impl_->WriteImpl(write_options_, working_batch, nullptr,
                          /*user_write_cb=*/nullptr, nullptr, zero_log_number,
                          disable_memtable, &seq_used, batch_cnt,
                          pre_release_callback);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  const SequenceNumber commit_batch_seq = seq_used;
  if (LIKELY(do_one_write || !s.ok())) {
    if (UNLIKELY(!db_impl_->immutable_db_options().two_write_queues &&
                 s.ok())) {
      // Note: RemovePrepared should be called after WriteImpl that publishsed
      // the seq. Otherwise SmallestUnCommittedSeq optimization breaks.
      wpt_db_->RemovePrepared(prepare_seq, prepare_batch_cnt_);
    }  // else RemovePrepared is called from within PreReleaseCallback
    if (UNLIKELY(!do_one_write)) {
      assert(!s.ok());
      // Cleanup the prepared entry we added with add_prepared_callback
      wpt_db_->RemovePrepared(commit_batch_seq, commit_batch_cnt);
    }
    return s;
  }  // else do the 2nd write to publish seq
  // Note: the 2nd write comes with a performance penality. So if we have too
  // many of commits accompanied with ComitTimeWriteBatch and yet we cannot
  // enable use_only_the_last_commit_time_batch_for_recovery_ optimization,
  // two_write_queues should be disabled to avoid many additional writes here.
  const size_t kZeroData = 0;
  // Update commit map only from the 2nd queue
  WritePreparedCommitEntryPreReleaseCallback update_commit_map_with_aux_batch(
      wpt_db_, db_impl_, prepare_seq, prepare_batch_cnt_, kZeroData,
      commit_batch_seq, commit_batch_cnt);
  WriteBatch empty_batch;
  s = empty_batch.PutLogData(Slice());
  assert(s.ok());
  // In the absence of Prepare markers, use Noop as a batch separator
  s = WriteBatchInternal::InsertNoop(&empty_batch);
  assert(s.ok());
  const bool DISABLE_MEMTABLE = true;
  const size_t ONE_BATCH = 1;
  const uint64_t NO_REF_LOG = 0;
  s = db_impl_->WriteImpl(write_options_, &empty_batch, nullptr,
                          /*user_write_cb=*/nullptr, nullptr, NO_REF_LOG,
                          DISABLE_MEMTABLE, &seq_used, ONE_BATCH,
                          &update_commit_map_with_aux_batch);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  return s;
}

Status WritePreparedTxn::RollbackInternal() {
  ROCKS_LOG_WARN(db_impl_->immutable_db_options().info_log,
                 "RollbackInternal prepare_seq: %" PRIu64, GetId());

  assert(db_impl_);
  assert(wpt_db_);

  WriteBatch rollback_batch(0 /* reserved_bytes */, 0 /* max_bytes */,
                            write_options_.protection_bytes_per_key,
                            0 /* default_cf_ts_sz */);
  assert(GetId() != kMaxSequenceNumber);
  assert(GetId() > 0);
  auto cf_map_shared_ptr = wpt_db_->GetCFHandleMap();
  auto cf_comp_map_shared_ptr = wpt_db_->GetCFComparatorMap();
  auto read_at_seq = kMaxSequenceNumber;
  // TODO: plumb Env::IOActivity, Env::IOPriority
  ReadOptions roptions;
  // to prevent callback's seq to be overrriden inside DBImpk::Get
  roptions.snapshot = wpt_db_->GetMaxSnapshot();
  struct RollbackWriteBatchBuilder : public WriteBatch::Handler {
    DBImpl* const db_;
    WritePreparedTxnDB* const wpt_db_;
    WritePreparedTxnReadCallback callback_;
    WriteBatch* rollback_batch_;
    std::map<uint32_t, const Comparator*>& comparators_;
    std::map<uint32_t, ColumnFamilyHandle*>& handles_;
    using CFKeys = std::set<Slice, SetComparator>;
    std::map<uint32_t, CFKeys> keys_;
    bool rollback_merge_operands_;
    ReadOptions roptions_;

    RollbackWriteBatchBuilder(
        DBImpl* db, WritePreparedTxnDB* wpt_db, SequenceNumber snap_seq,
        WriteBatch* dst_batch,
        std::map<uint32_t, const Comparator*>& comparators,
        std::map<uint32_t, ColumnFamilyHandle*>& handles,
        bool rollback_merge_operands, const ReadOptions& _roptions)
        : db_(db),
          wpt_db_(wpt_db),
          callback_(wpt_db, snap_seq),  // disable min_uncommitted optimization
          rollback_batch_(dst_batch),
          comparators_(comparators),
          handles_(handles),
          rollback_merge_operands_(rollback_merge_operands),
          roptions_(_roptions) {}

    Status Rollback(uint32_t cf, const Slice& key) {
      Status s;
      CFKeys& cf_keys = keys_[cf];
      if (cf_keys.size() == 0) {  // just inserted
        auto cmp = comparators_[cf];
        keys_[cf] = CFKeys(SetComparator(cmp));
      }
      auto it = cf_keys.insert(key);
      // second is false if a element already existed.
      if (it.second == false) {
        return s;
      }

      PinnableSlice pinnable_val;
      bool not_used;
      auto cf_handle = handles_[cf];
      DBImpl::GetImplOptions get_impl_options;
      get_impl_options.column_family = cf_handle;
      get_impl_options.value = &pinnable_val;
      get_impl_options.value_found = &not_used;
      get_impl_options.callback = &callback_;
      s = db_->GetImpl(roptions_, key, get_impl_options);
      assert(s.ok() || s.IsNotFound());
      if (s.ok()) {
        s = rollback_batch_->Put(cf_handle, key, pinnable_val);
        assert(s.ok());
      } else if (s.IsNotFound()) {
        // There has been no readable value before txn. By adding a delete we
        // make sure that there will be none afterwards either.
        if (wpt_db_->ShouldRollbackWithSingleDelete(cf_handle, key)) {
          s = rollback_batch_->SingleDelete(cf_handle, key);
        } else {
          s = rollback_batch_->Delete(cf_handle, key);
        }
        assert(s.ok());
      } else {
        // Unexpected status. Return it to the user.
      }
      return s;
    }

    Status PutCF(uint32_t cf, const Slice& key, const Slice& /*val*/) override {
      return Rollback(cf, key);
    }

    Status DeleteCF(uint32_t cf, const Slice& key) override {
      return Rollback(cf, key);
    }

    Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
      return Rollback(cf, key);
    }

    Status MergeCF(uint32_t cf, const Slice& key,
                   const Slice& /*val*/) override {
      if (rollback_merge_operands_) {
        return Rollback(cf, key);
      } else {
        return Status::OK();
      }
    }

    Status MarkNoop(bool) override { return Status::OK(); }
    Status MarkBeginPrepare(bool) override { return Status::OK(); }
    Status MarkEndPrepare(const Slice&) override { return Status::OK(); }
    Status MarkCommit(const Slice&) override { return Status::OK(); }
    Status MarkRollback(const Slice&) override {
      return Status::InvalidArgument();
    }

   protected:
    Handler::OptionState WriteAfterCommit() const override {
      return Handler::OptionState::kDisabled;
    }
  } rollback_handler(db_impl_, wpt_db_, read_at_seq, &rollback_batch,
                     *cf_comp_map_shared_ptr.get(), *cf_map_shared_ptr.get(),
                     wpt_db_->txn_db_options_.rollback_merge_operands,
                     roptions);
  auto s = GetWriteBatch()->GetWriteBatch()->Iterate(&rollback_handler);
  if (!s.ok()) {
    return s;
  }
  // The Rollback marker will be used as a batch separator
  s = WriteBatchInternal::MarkRollback(&rollback_batch, name_);
  assert(s.ok());
  bool do_one_write = !db_impl_->immutable_db_options().two_write_queues;
  const bool DISABLE_MEMTABLE = true;
  const uint64_t NO_REF_LOG = 0;
  uint64_t seq_used = kMaxSequenceNumber;
  const size_t ONE_BATCH = 1;
  const bool kFirstPrepareBatch = true;
  // We commit the rolled back prepared batches. Although this is
  // counter-intuitive, i) it is safe to do so, since the prepared batches are
  // already canceled out by the rollback batch, ii) adding the commit entry to
  // CommitCache will allow us to benefit from the existing mechanism in
  // CommitCache that keeps an entry evicted due to max advance and yet overlaps
  // with a live snapshot around so that the live snapshot properly skips the
  // entry even if its prepare seq is lower than max_evicted_seq_.
  AddPreparedCallback add_prepared_callback(
      wpt_db_, db_impl_, ONE_BATCH,
      db_impl_->immutable_db_options().two_write_queues, !kFirstPrepareBatch);
  WritePreparedCommitEntryPreReleaseCallback update_commit_map(
      wpt_db_, db_impl_, GetId(), prepare_batch_cnt_, ONE_BATCH);
  PreReleaseCallback* pre_release_callback;
  if (do_one_write) {
    pre_release_callback = &update_commit_map;
  } else {
    pre_release_callback = &add_prepared_callback;
  }
  // Note: the rollback batch does not need AddPrepared since it is written to
  // DB in one shot. min_uncommitted still works since it requires capturing
  // data that is written to DB but not yet committed, while
  // the rollback batch commits with PreReleaseCallback.
  s = db_impl_->WriteImpl(write_options_, &rollback_batch, nullptr,
                          /*user_write_cb=*/nullptr, nullptr, NO_REF_LOG,
                          !DISABLE_MEMTABLE, &seq_used, ONE_BATCH,
                          pre_release_callback);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  if (!s.ok()) {
    return s;
  }
  if (do_one_write) {
    assert(!db_impl_->immutable_db_options().two_write_queues);
    wpt_db_->RemovePrepared(GetId(), prepare_batch_cnt_);
    return s;
  }  // else do the 2nd write for commit
  uint64_t rollback_seq = seq_used;
  ROCKS_LOG_DETAILS(db_impl_->immutable_db_options().info_log,
                    "RollbackInternal 2nd write rollback_seq: %" PRIu64,
                    rollback_seq);
  // Commit the batch by writing an empty batch to the queue that will release
  // the commit sequence number to readers.
  WritePreparedRollbackPreReleaseCallback update_commit_map_with_prepare(
      wpt_db_, db_impl_, GetId(), rollback_seq, prepare_batch_cnt_);
  WriteBatch empty_batch;
  s = empty_batch.PutLogData(Slice());
  assert(s.ok());
  // In the absence of Prepare markers, use Noop as a batch separator
  s = WriteBatchInternal::InsertNoop(&empty_batch);
  assert(s.ok());
  s = db_impl_->WriteImpl(write_options_, &empty_batch, nullptr,
                          /*user_write_cb=*/nullptr, nullptr, NO_REF_LOG,
                          DISABLE_MEMTABLE, &seq_used, ONE_BATCH,
                          &update_commit_map_with_prepare);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  ROCKS_LOG_DETAILS(db_impl_->immutable_db_options().info_log,
                    "RollbackInternal (status=%s) commit: %" PRIu64,
                    s.ToString().c_str(), GetId());
  // TODO(lth): For WriteUnPrepared that rollback is called frequently,
  // RemovePrepared could be moved to the callback to reduce lock contention.
  if (s.ok()) {
    wpt_db_->RemovePrepared(GetId(), prepare_batch_cnt_);
  }
  // Note: RemovePrepared for prepared batch is called from within
  // PreReleaseCallback
  wpt_db_->RemovePrepared(rollback_seq, ONE_BATCH);

  return s;
}

Status WritePreparedTxn::ValidateSnapshot(ColumnFamilyHandle* column_family,
                                          const Slice& key,
                                          SequenceNumber* tracked_at_seq) {
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

  WritePreparedTxnReadCallback snap_checker(wpt_db_, snap_seq, min_uncommitted,
                                            kBackedByDBSnapshot);
  // TODO(yanqin): support user-defined timestamp
  return TransactionUtil::CheckKeyForConflicts(
      db_impl_, cfh, key.ToString(), snap_seq, /*ts=*/nullptr,
      false /* cache_only */, &snap_checker, min_uncommitted,
      txn_db_impl_->GetTxnDBOptions().enable_udt_validation);
}

void WritePreparedTxn::SetSnapshot() {
  const bool kForWWConflictCheck = true;
  SnapshotImpl* snapshot = wpt_db_->GetSnapshotInternal(kForWWConflictCheck);
  SetSnapshotInternal(snapshot);
}

Status WritePreparedTxn::RebuildFromWriteBatch(WriteBatch* src_batch) {
  auto ret = PessimisticTransaction::RebuildFromWriteBatch(src_batch);
  prepare_batch_cnt_ = GetWriteBatch()->SubBatchCnt();
  return ret;
}

}  // namespace ROCKSDB_NAMESPACE
