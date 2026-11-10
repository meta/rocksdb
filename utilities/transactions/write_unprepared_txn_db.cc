//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/transactions/write_unprepared_txn_db.h"

#include "db/arena_wrapped_db_iter.h"
#include "rocksdb/utilities/transaction_db.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

// Instead of reconstructing a Transaction object, and calling rollback on it,
// we can be more efficient with RollbackRecoveredTransaction by skipping
// unnecessary steps (eg. updating CommitMap, reconstructing keyset)
Status WriteUnpreparedTxnDB::RollbackRecoveredTransaction(
    const DBImpl::RecoveredTransaction* rtxn) {
  // TODO(lth): Reduce duplicate code with WritePrepared rollback logic.
  assert(rtxn->unprepared_);
  auto cf_map_shared_ptr = WritePreparedTxnDB::GetCFHandleMap();
  auto cf_comp_map_shared_ptr = WritePreparedTxnDB::GetCFComparatorMap();
  // In theory we could write with disableWAL = true during recovery, and
  // assume that if we crash again during recovery, we can just replay from
  // the very beginning. Unfortunately, the XIDs from the application may not
  // necessarily be unique across restarts, potentially leading to situations
  // like this:
  //
  // BEGIN_PREPARE(unprepared) Put(a) END_PREPARE(xid = 1)
  // -- crash and recover with Put(a) rolled back as it was not prepared
  // BEGIN_PREPARE(prepared) Put(b) END_PREPARE(xid = 1)
  // COMMIT(xid = 1)
  // -- crash and recover with both a, b
  //
  // We could just write the rollback marker, but then we would have to extend
  // MemTableInserter during recovery to actually do writes into the DB
  // instead of just dropping the in-memory write batch.
  //
  // TODO: plumb Env::IOActivity, Env::IOPriority
  WriteOptions w_options;

  class InvalidSnapshotReadCallback : public ReadCallback {
   public:
    explicit InvalidSnapshotReadCallback(SequenceNumber snapshot)
        : ReadCallback(snapshot) {}

    inline bool IsVisibleFullCheck(SequenceNumber) override {
      // The seq provided as snapshot is the seq right before we have locked and
      // wrote to it, so whatever is there, it is committed.
      return true;
    }

    // Ignore the refresh request since we are confident that our snapshot seq
    // is not going to be affected by concurrent compactions (not enabled yet.)
    void Refresh(SequenceNumber) override {}
  };

  // Iterate starting with largest sequence number.
  for (auto it = rtxn->batches_.rbegin(); it != rtxn->batches_.rend(); ++it) {
    auto last_visible_txn = it->first - 1;
    const auto& batch = it->second.batch_;
    WriteBatch rollback_batch(0 /* reserved_bytes */, 0 /* max_bytes */,
                              w_options.protection_bytes_per_key,
                              0 /* default_cf_ts_sz */);

    struct RollbackWriteBatchBuilder : public WriteBatch::Handler {
      DBImpl* db_;
      ReadOptions roptions;
      InvalidSnapshotReadCallback callback;
      WriteBatch* rollback_batch_;
      std::map<uint32_t, const Comparator*>& comparators_;
      std::map<uint32_t, ColumnFamilyHandle*>& handles_;
      using CFKeys = std::set<Slice, SetComparator>;
      std::map<uint32_t, CFKeys> keys_;
      bool rollback_merge_operands_;
      RollbackWriteBatchBuilder(
          DBImpl* db, SequenceNumber snap_seq, WriteBatch* dst_batch,
          std::map<uint32_t, const Comparator*>& comparators,
          std::map<uint32_t, ColumnFamilyHandle*>& handles,
          bool rollback_merge_operands)
          : db_(db),
            callback(snap_seq),
            // disable min_uncommitted optimization
            rollback_batch_(dst_batch),
            comparators_(comparators),
            handles_(handles),
            rollback_merge_operands_(rollback_merge_operands) {}

      Status Rollback(uint32_t cf, const Slice& key) {
        Status s;
        CFKeys& cf_keys = keys_[cf];
        if (cf_keys.size() == 0) {  // just inserted
          auto cmp = comparators_[cf];
          keys_[cf] = CFKeys(SetComparator(cmp));
        }
        auto res = cf_keys.insert(key);
        if (res.second ==
            false) {  // second is false if a element already existed.
          return s;
        }

        PinnableSlice pinnable_val;
        bool not_used;
        auto cf_handle = handles_[cf];
        DBImpl::GetImplOptions get_impl_options;
        get_impl_options.column_family = cf_handle;
        get_impl_options.value = &pinnable_val;
        get_impl_options.value_found = &not_used;
        get_impl_options.callback = &callback;
        s = db_->GetImpl(roptions, key, get_impl_options);
        assert(s.ok() || s.IsNotFound());
        if (s.ok()) {
          s = rollback_batch_->Put(cf_handle, key, pinnable_val);
          assert(s.ok());
        } else if (s.IsNotFound()) {
          // There has been no readable value before txn. By adding a delete we
          // make sure that there will be none afterwards either.
          s = rollback_batch_->Delete(cf_handle, key);
          assert(s.ok());
        } else {
          // Unexpected status. Return it to the user.
        }
        return s;
      }

      Status PutCF(uint32_t cf, const Slice& key,
                   const Slice& /*val*/) override {
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

      // Recovered batches do not contain 2PC markers.
      Status MarkNoop(bool) override { return Status::InvalidArgument(); }
      Status MarkBeginPrepare(bool) override {
        return Status::InvalidArgument();
      }
      Status MarkEndPrepare(const Slice&) override {
        return Status::InvalidArgument();
      }
      Status MarkCommit(const Slice&) override {
        return Status::InvalidArgument();
      }
      Status MarkRollback(const Slice&) override {
        return Status::InvalidArgument();
      }
    } rollback_handler(db_impl_, last_visible_txn, &rollback_batch,
                       *cf_comp_map_shared_ptr.get(), *cf_map_shared_ptr.get(),
                       txn_db_options_.rollback_merge_operands);

    auto s = batch->Iterate(&rollback_handler);
    if (!s.ok()) {
      return s;
    }

    // The Rollback marker will be used as a batch separator
    s = WriteBatchInternal::MarkRollback(&rollback_batch, rtxn->name_);
    if (!s.ok()) {
      return s;
    }

    const uint64_t kNoLogRef = 0;
    const bool kDisableMemtable = true;
    const size_t kOneBatch = 1;
    uint64_t seq_used = kMaxSequenceNumber;
    s = db_impl_->WriteImpl(w_options, &rollback_batch, nullptr, nullptr,
                            nullptr, kNoLogRef, !kDisableMemtable, &seq_used,
                            kOneBatch);
    if (!s.ok()) {
      return s;
    }

    // If two_write_queues, we must manually release the sequence number to
    // readers.
    if (db_impl_->immutable_db_options().two_write_queues) {
      db_impl_->SetLastPublishedSequence(seq_used);
    }
  }

  return Status::OK();
}

Status WriteUnpreparedTxnDB::Initialize(
    const std::vector<size_t>& compaction_enabled_cf_indices,
    const std::vector<ColumnFamilyHandle*>& handles) {
  // TODO(lth): Reduce code duplication in this function.
  auto dbimpl = static_cast_with_check<DBImpl>(GetRootDB());
  assert(dbimpl != nullptr);

  db_impl_->SetSnapshotChecker(new WritePreparedSnapshotChecker(this));
  // A callback to commit a single sub-batch
  class CommitSubBatchPreReleaseCallback : public PreReleaseCallback {
   public:
    explicit CommitSubBatchPreReleaseCallback(WritePreparedTxnDB* db)
        : db_(db) {}
    Status Callback(SequenceNumber commit_seq,
                    bool is_mem_disabled __attribute__((__unused__)), uint64_t,
                    size_t /*index*/, size_t /*total*/) override {
      assert(!is_mem_disabled);
      db_->AddCommitted(commit_seq, commit_seq);
      return Status::OK();
    }

   private:
    WritePreparedTxnDB* db_;
  };
  db_impl_->SetRecoverableStatePreReleaseCallback(
      new CommitSubBatchPreReleaseCallback(this));

  // PessimisticTransactionDB::Initialize
  for (auto cf_ptr : handles) {
    AddColumnFamily(cf_ptr);
  }
  // Verify cf options
  for (auto handle : handles) {
    ColumnFamilyDescriptor cfd;
    Status s = handle->GetDescriptor(&cfd);
    if (!s.ok()) {
      return s;
    }
    s = VerifyCFOptions(cfd.options);
    if (!s.ok()) {
      return s;
    }
  }

  // Re-enable compaction for the column families that initially had
  // compaction enabled.
  std::vector<ColumnFamilyHandle*> compaction_enabled_cf_handles;
  compaction_enabled_cf_handles.reserve(compaction_enabled_cf_indices.size());
  for (auto index : compaction_enabled_cf_indices) {
    compaction_enabled_cf_handles.push_back(handles[index]);
  }

  // create 'real' transactions from recovered shell transactions
  auto rtxns = dbimpl->recovered_transactions();
  std::map<SequenceNumber, SequenceNumber> ordered_seq_cnt;
  for (const auto& rtxn : rtxns) {
    auto recovered_trx = rtxn.second;
    assert(recovered_trx);
    assert(recovered_trx->batches_.size() >= 1);
    assert(recovered_trx->name_.length());

    // We can only rollback transactions after AdvanceMaxEvictedSeq is called,
    // but AddPrepared must occur before AdvanceMaxEvictedSeq, which is why
    // two iterations is required.
    if (recovered_trx->unprepared_) {
      continue;
    }

    // TODO: plumb Env::IOActivity, Env::IOPriority
    WriteOptions w_options;
    w_options.sync = true;
    TransactionOptions t_options;

    auto first_log_number = recovered_trx->batches_.begin()->second.log_number_;
    auto first_seq = recovered_trx->batches_.begin()->first;
    auto last_prepare_batch_cnt =
        recovered_trx->batches_.begin()->second.batch_cnt_;

    Transaction* real_trx = BeginTransaction(w_options, t_options, nullptr);
    assert(real_trx);
    auto wupt = static_cast_with_check<WriteUnpreparedTxn>(real_trx);
    wupt->recovered_txn_ = true;

    real_trx->SetLogNumber(first_log_number);
    real_trx->SetId(first_seq);
    Status s = real_trx->SetName(recovered_trx->name_);
    if (!s.ok()) {
      return s;
    }
    wupt->prepare_batch_cnt_ = last_prepare_batch_cnt;

    for (auto batch : recovered_trx->batches_) {
      const auto& seq = batch.first;
      const auto& batch_info = batch.second;
      auto cnt = batch_info.batch_cnt_ ? batch_info.batch_cnt_ : 1;
      assert(batch_info.log_number_);

      ordered_seq_cnt[seq] = cnt;
      assert(wupt->unprep_seqs_.count(seq) == 0);
      wupt->unprep_seqs_[seq] = cnt;

      s = wupt->RebuildFromWriteBatch(batch_info.batch_);
      assert(s.ok());
      if (!s.ok()) {
        return s;
      }
    }

    const bool kClear = true;
    wupt->InitWriteBatch(kClear);

    real_trx->SetState(Transaction::PREPARED);
    if (!s.ok()) {
      return s;
    }
  }
  // AddPrepared must be called in order
  for (auto seq_cnt : ordered_seq_cnt) {
    auto seq = seq_cnt.first;
    auto cnt = seq_cnt.second;
    for (size_t i = 0; i < cnt; i++) {
      AddPrepared(seq + i);
    }
  }

  SequenceNumber prev_max = max_evicted_seq_;
  SequenceNumber last_seq = db_impl_->GetLatestSequenceNumber();
  AdvanceMaxEvictedSeq(prev_max, last_seq);
  // Create a gap between max and the next snapshot. This simplifies the logic
  // in IsInSnapshot by not having to consider the special case of max ==
  // snapshot after recovery. This is tested in IsInSnapshotEmptyMapTest.
  if (last_seq) {
    db_impl_->versions_->SetLastAllocatedSequence(last_seq + 1);
    db_impl_->versions_->SetLastSequence(last_seq + 1);
    db_impl_->versions_->SetLastPublishedSequence(last_seq + 1);
  }

  Status s;
  // Rollback unprepared transactions.
  for (const auto& rtxn : rtxns) {
    auto recovered_trx = rtxn.second;
    if (recovered_trx->unprepared_) {
      s = RollbackRecoveredTransaction(recovered_trx);
      if (!s.ok()) {
        return s;
      }
      continue;
    }
  }

  if (s.ok()) {
    dbimpl->DeleteAllRecoveredTransactions();

    // Compaction should start only after max_evicted_seq_ is set AND recovered
    // transactions are either added to PrepareHeap or rolled back.
    s = EnableAutoCompaction(compaction_enabled_cf_handles);
  }

  return s;
}

Transaction* WriteUnpreparedTxnDB::BeginTransaction(
    const WriteOptions& write_options, const TransactionOptions& txn_options,
    Transaction* old_txn) {
  if (old_txn != nullptr) {
    ReinitializeTransaction(old_txn, write_options, txn_options);
    return old_txn;
  } else {
    return new WriteUnpreparedTxn(this, write_options, txn_options);
  }
}

// Struct to hold ownership of snapshot and read callback for iterator cleanup.
struct WriteUnpreparedTxnDB::IteratorState {
  IteratorState(WritePreparedTxnDB* txn_db, SequenceNumber sequence,
                std::shared_ptr<ManagedSnapshot> s,
                SequenceNumber min_uncommitted, WriteUnpreparedTxn* txn)
      : callback(txn_db, sequence, min_uncommitted, txn->unprep_seqs_,
                 kBackedByDBSnapshot),
        snapshot(s) {}
  SequenceNumber MaxVisibleSeq() { return callback.max_visible_seq(); }

  WriteUnpreparedTxnReadCallback callback;
  std::shared_ptr<ManagedSnapshot> snapshot;
};

namespace {
static void CleanupWriteUnpreparedTxnDBIterator(void* arg1, void* /*arg2*/) {
  delete static_cast<WriteUnpreparedTxnDB::IteratorState*>(arg1);
}
}  // anonymous namespace

Iterator* WriteUnpreparedTxnDB::NewIterator(const ReadOptions& _read_options,
                                            ColumnFamilyHandle* column_family,
                                            WriteUnpreparedTxn* txn) {
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
  // TODO(lth): Refactor so that this logic is shared with WritePrepared.
  constexpr bool expose_blob_index = false;
  constexpr bool allow_refresh = false;
  std::shared_ptr<ManagedSnapshot> own_snapshot = nullptr;
  SequenceNumber snapshot_seq = kMaxSequenceNumber;
  SequenceNumber min_uncommitted = 0;

  // Currently, the Prev() iterator logic does not work well without snapshot
  // validation. The logic simply iterates through values of a key in
  // ascending seqno order, stopping at the first non-visible value and
  // returning the last visible value.
  //
  // For example, if snapshot sequence is 3, and we have the following keys:
  // foo: v1 1
  // foo: v2 2
  // foo: v3 3
  // foo: v4 4
  // foo: v5 5
  //
  // Then 1, 2, 3 will be visible, but 4 will be non-visible, so we return v3,
  // which is the last visible value.
  //
  // For unprepared transactions, if we have snap_seq = 3, but the current
  // transaction has unprep_seq 5, then returning the first non-visible value
  // would be incorrect, as we should return v5, and not v3. The problem is that
  // there are committed values at snapshot_seq < commit_seq < unprep_seq.
  //
  // Snapshot validation can prevent this problem by ensuring that no committed
  // values exist at snapshot_seq < commit_seq, and thus any value with a
  // sequence number greater than snapshot_seq must be unprepared values. For
  // example, if the transaction had a snapshot at 3, then snapshot validation
  // would be performed during the Put(v5) call. It would find v4, and the Put
  // would fail with snapshot validation failure.
  //
  // TODO(lth): Improve Prev() logic to continue iterating until
  // max_visible_seq, and then return the last visible value, so that this
  // restriction can be lifted.
  const Snapshot* snapshot = nullptr;
  if (read_options.snapshot == nullptr) {
    snapshot = GetSnapshot();
    own_snapshot = std::make_shared<ManagedSnapshot>(db_impl_, snapshot);
  } else {
    snapshot = read_options.snapshot;
  }

  snapshot_seq = snapshot->GetSequenceNumber();
  assert(snapshot_seq != kMaxSequenceNumber);
  // Iteration is safe as long as largest_validated_seq <= snapshot_seq. We are
  // guaranteed that for keys that were modified by this transaction (and thus
  // might have unprepared values), no committed values exist at
  // largest_validated_seq < commit_seq (or the contrapositive: any committed
  // value must exist at commit_seq <= largest_validated_seq). This implies
  // that commit_seq <= largest_validated_seq <= snapshot_seq or commit_seq <=
  // snapshot_seq. As explained above, the problem with Prev() only happens when
  // snapshot_seq < commit_seq.
  //
  // For keys that were not modified by this transaction, largest_validated_seq_
  // is meaningless, and Prev() should just work with the existing visibility
  // logic.
  if (txn->largest_validated_seq_ > snapshot->GetSequenceNumber() &&
      !txn->unprep_seqs_.empty()) {
    ROCKS_LOG_ERROR(info_log_,
                    "WriteUnprepared iterator creation failed since the "
                    "transaction has performed unvalidated writes");
    return nullptr;
  }
  min_uncommitted =
      static_cast_with_check<const SnapshotImpl>(snapshot)->min_uncommitted_;

  auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto* cfd = cfh->cfd();
  auto* state =
      new IteratorState(this, snapshot_seq, own_snapshot, min_uncommitted, txn);
  SuperVersion* super_version = cfd->GetReferencedSuperVersion(db_impl_);
  auto* db_iter = db_impl_->NewIteratorImpl(
      read_options, cfh, super_version, state->MaxVisibleSeq(),
      &state->callback, expose_blob_index, allow_refresh);
  db_iter->RegisterCleanup(CleanupWriteUnpreparedTxnDBIterator, state, nullptr);
  return db_iter;
}

}  // namespace ROCKSDB_NAMESPACE
