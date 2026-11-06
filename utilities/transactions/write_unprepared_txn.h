// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <set>

#include "utilities/transactions/write_prepared_txn.h"
#include "utilities/transactions/write_unprepared_txn_db.h"

namespace ROCKSDB_NAMESPACE {

class WriteUnpreparedTxnDB;
class WriteUnpreparedTxn;

// WriteUnprepared transactions needs to be able to read their own uncommitted
// writes, and supporting this requires some careful consideration. Because
// writes in the current transaction may be flushed to DB already, we cannot
// rely on the contents of WriteBatchWithIndex to determine whether a key should
// be visible or not, so we have to remember to check the DB for any uncommitted
// keys that should be visible to us. First, we will need to change the seek to
// snapshot logic, to seek to max_visible_seq = max(snap_seq, max_unprep_seq).
// Any key greater than max_visible_seq should not be visible because they
// cannot be unprepared by the current transaction and they are not in its
// snapshot.
//
// When we seek to max_visible_seq, one of these cases will happen:
// 1. We hit a unprepared key from the current transaction.
// 2. We hit a unprepared key from the another transaction.
// 3. We hit a committed key with snap_seq < seq < max_unprep_seq.
// 4. We hit a committed key with seq <= snap_seq.
//
// IsVisibleFullCheck handles all cases correctly.
//
// Other notes:
// Note that max_visible_seq is only calculated once at iterator construction
// time, meaning if the same transaction is adding more unprep seqs through
// writes during iteration, these newer writes may not be visible. This is not a
// problem for MySQL though because it avoids modifying the index as it is
// scanning through it to avoid the Halloween Problem. Instead, it scans the
// index once up front, and modifies based on a temporary copy.
//
// In DBIter, there is a "reseek" optimization if the iterator skips over too
// many keys. However, this assumes that the reseek seeks exactly to the
// required key. In write unprepared, even after seeking directly to
// max_visible_seq, some iteration may be required before hitting a visible key,
// and special precautions must be taken to avoid performing another reseek,
// leading to an infinite loop.
//
class WriteUnpreparedTxnReadCallback : public ReadCallback {
 public:
  WriteUnpreparedTxnReadCallback(
      WritePreparedTxnDB* db, SequenceNumber snapshot,
      SequenceNumber min_uncommitted,
      const std::map<SequenceNumber, size_t>& unprep_seqs,
      SnapshotBackup backed_by_snapshot)
      // Pass our last uncommitted seq as the snapshot to the parent class to
      // ensure that the parent will not prematurely filter out own writes. We
      // will do the exact comparison against snapshots in IsVisibleFullCheck
      // override.
      : ReadCallback(CalcMaxVisibleSeq(unprep_seqs, snapshot), min_uncommitted),
        db_(db),
        unprep_seqs_(unprep_seqs),
        wup_snapshot_(snapshot),
        backed_by_snapshot_(backed_by_snapshot) {
    (void)backed_by_snapshot_;  // to silence unused private field warning
  }

  // No copying allowed
  WriteUnpreparedTxnReadCallback(const WriteUnpreparedTxnReadCallback&) =
      delete;
  WriteUnpreparedTxnReadCallback& operator=(
      const WriteUnpreparedTxnReadCallback&) = delete;
  WriteUnpreparedTxnReadCallback(WriteUnpreparedTxnReadCallback&&) = delete;
  WriteUnpreparedTxnReadCallback& operator=(WriteUnpreparedTxnReadCallback&&) =
      delete;

  ~WriteUnpreparedTxnReadCallback() override {
    // If it is not backed by snapshot, the caller must check validity
    assert(valid_checked_ || backed_by_snapshot_ == kBackedByDBSnapshot);
  }

  bool IsVisibleFullCheck(SequenceNumber seq) override;

  inline bool valid() {
    valid_checked_ = true;
    return snap_released_ == false;
  }

  void Refresh(SequenceNumber seq) override {
    max_visible_seq_ = std::max(max_visible_seq_, seq);
    wup_snapshot_ = seq;
  }

  static SequenceNumber CalcMaxVisibleSeq(
      const std::map<SequenceNumber, size_t>& unprep_seqs,
      SequenceNumber snapshot_seq) {
    SequenceNumber max_unprepared = 0;
    if (unprep_seqs.size()) {
      max_unprepared =
          unprep_seqs.rbegin()->first + unprep_seqs.rbegin()->second - 1;
    }
    return std::max(max_unprepared, snapshot_seq);
  }

 private:
  WritePreparedTxnDB* db_;
  const std::map<SequenceNumber, size_t>& unprep_seqs_;
  SequenceNumber wup_snapshot_;
  // Whether max_visible_seq_ is backed by a snapshot
  const SnapshotBackup backed_by_snapshot_;
  bool snap_released_ = false;
  // Safety check to ensure that the caller has checked invalid statuses
  bool valid_checked_ = false;
};

class WriteUnpreparedTxn : public WritePreparedTxn {
 public:
  WriteUnpreparedTxn(WriteUnpreparedTxnDB* db,
                     const WriteOptions& write_options,
                     const TransactionOptions& txn_options);

  ~WriteUnpreparedTxn() override;

  using TransactionBaseImpl::Put;
  Status Put(ColumnFamilyHandle* column_family, const Slice& key,
             const Slice& value, const bool assume_tracked = false) override;
  Status Put(ColumnFamilyHandle* column_family, const SliceParts& key,
             const SliceParts& value,
             const bool assume_tracked = false) override;

  using TransactionBaseImpl::Merge;
  Status Merge(ColumnFamilyHandle* column_family, const Slice& key,
               const Slice& value, const bool assume_tracked = false) override;

  using TransactionBaseImpl::Delete;
  Status Delete(ColumnFamilyHandle* column_family, const Slice& key,
                const bool assume_tracked = false) override;
  Status Delete(ColumnFamilyHandle* column_family, const SliceParts& key,
                const bool assume_tracked = false) override;

  using TransactionBaseImpl::SingleDelete;
  Status SingleDelete(ColumnFamilyHandle* column_family, const Slice& key,
                      const bool assume_tracked = false) override;
  Status SingleDelete(ColumnFamilyHandle* column_family, const SliceParts& key,
                      const bool assume_tracked = false) override;

  // In WriteUnprepared, untracked writes will break snapshot validation logic.
  // Snapshot validation will only check the largest sequence number of a key to
  // see if it was committed or not. However, an untracked unprepared write will
  // hide smaller committed sequence numbers.
  //
  // TODO(lth): Investigate whether it is worth having snapshot validation
  // validate all values larger than snap_seq. Otherwise, we should return
  // Status::NotSupported for untracked writes.

  Status RebuildFromWriteBatch(WriteBatch*) override;

  uint64_t GetLastLogNumber() const override { return last_log_number_; }

  void RemoveActiveIterator(Iterator* iter) {
    active_iterators_.erase(
        std::remove(active_iterators_.begin(), active_iterators_.end(), iter),
        active_iterators_.end());
  }

 protected:
  void Initialize(const TransactionOptions& txn_options) override;

  Status PrepareInternal() override;

  Status CommitWithoutPrepareInternal() override;
  Status CommitInternal() override;

  Status RollbackInternal() override;

  void Clear() override;

  void SetSavePoint() override;
  Status RollbackToSavePoint() override;
  Status PopSavePoint() override;

  // Read APIs need to be overridden so that a ReadCallback handles
  // read-your-own-write.
  using Transaction::Get;
  Status Get(const ReadOptions& _read_options,
             ColumnFamilyHandle* column_family, const Slice& key,
             PinnableSlice* value) override;

  using Transaction::GetIterator;
  Iterator* GetIterator(const ReadOptions& options) override;
  Iterator* GetIterator(const ReadOptions& options,
                        ColumnFamilyHandle* column_family) override;

  Status ValidateSnapshot(ColumnFamilyHandle* column_family, const Slice& key,
                          SequenceNumber* tracked_at_seq) override;

 private:
  friend class WriteUnpreparedTransactionTest_ReadYourOwnWrite_Test;
  friend class WriteUnpreparedTransactionTest_RecoveryTest_Test;
  friend class WriteUnpreparedTransactionTest_UnpreparedBatch_Test;
  friend class WriteUnpreparedTxnDB;

  const std::map<SequenceNumber, size_t>& GetUnpreparedSequenceNumbers();
  using Transaction::GetImpl;
  Status GetImpl(const ReadOptions& options, ColumnFamilyHandle* column_family,
                 const Slice& key, PinnableSlice* value) override;

  void MultiGetImpl(const ReadOptions& options,
                    ColumnFamilyHandle* column_family, size_t num_keys,
                    const Slice* keys, PinnableSlice* values, Status* statuses,
                    bool sorted_input) override;

  Status GetEntityImpl(const ReadOptions& options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       PinnableWideColumns* columns) override;

  void MultiGetEntityImpl(const ReadOptions& options,
                          ColumnFamilyHandle* column_family, size_t num_keys,
                          const Slice* keys, PinnableWideColumns* results,
                          Status* statuses, bool sorted_input) override;

  Status WriteRollbackKeys(const LockTracker& tracked_keys,
                           WriteBatchWithIndex* rollback_batch,
                           ReadCallback* callback, const ReadOptions& roptions);

  Status MaybeFlushWriteBatchToDB();
  Status FlushWriteBatchToDB(bool prepared);
  Status FlushWriteBatchToDBInternal(bool prepared);
  Status FlushWriteBatchWithSavePointToDB();
  Status RollbackToSavePointInternal();
  Status HandleWrite(std::function<Status()> do_write);

  // For write unprepared, we check on every writebatch append to see if
  // write_batch_flush_threshold_ has been exceeded, and then call
  // FlushWriteBatchToDB if so. This logic is encapsulated in
  // MaybeFlushWriteBatchToDB.
  int64_t write_batch_flush_threshold_;
  WriteUnpreparedTxnDB* wupt_db_;

  // Ordered list of unprep_seq sequence numbers that we have already written
  // to DB.
  //
  // This maps unprep_seq => prepare_batch_cnt for each unprepared batch
  // written by this transaction.
  //
  // Note that this contains both prepared and unprepared batches, since they
  // are treated similarily in prepare heap/commit map, so it simplifies the
  // commit callbacks.
  std::map<SequenceNumber, size_t> unprep_seqs_;

  uint64_t last_log_number_;

  // Recovered transactions have tracked_keys_ populated, but are not actually
  // locked for efficiency reasons. For recovered transactions, skip unlocking
  // keys when transaction ends.
  bool recovered_txn_;

  // Track the largest sequence number at which we performed snapshot
  // validation. If snapshot validation was skipped because no snapshot was set,
  // then this is set to GetLastPublishedSequence. This value is useful because
  // it means that for keys that have unprepared seqnos, we can guarantee that
  // no committed keys by other transactions can exist between
  // largest_validated_seq_ and max_unprep_seq. See
  // WriteUnpreparedTxnDB::NewIterator for an explanation for why this is
  // necessary for iterator Prev().
  //
  // Currently this value only increases during the lifetime of a transaction,
  // but in some cases, we should be able to restore the previously largest
  // value when calling RollbackToSavepoint.
  SequenceNumber largest_validated_seq_;

  struct SavePoint {
    // Record of unprep_seqs_ at this savepoint. The set of unprep_seq is
    // used during RollbackToSavepoint to determine visibility when restoring
    // old values.
    //
    // TODO(lth): Since all unprep_seqs_ sets further down the stack must be
    // subsets, this can potentially be deduplicated by just storing set
    // difference. Investigate if this is worth it.
    std::map<SequenceNumber, size_t> unprep_seqs_;

    // This snapshot will be used to read keys at this savepoint if we call
    // RollbackToSavePoint.
    std::unique_ptr<ManagedSnapshot> snapshot_;

    SavePoint(const std::map<SequenceNumber, size_t>& seqs,
              ManagedSnapshot* snapshot)
        : unprep_seqs_(seqs), snapshot_(snapshot) {}
  };

  // We have 3 data structures holding savepoint information:
  // 1. TransactionBaseImpl::save_points_
  // 2. WriteUnpreparedTxn::flushed_save_points_
  // 3. WriteUnpreparecTxn::unflushed_save_points_
  //
  // TransactionBaseImpl::save_points_ holds information about all write
  // batches, including the current in-memory write_batch_, or unprepared
  // batches that have been written out. Its responsibility is just to track
  // which keys have been modified in every savepoint.
  //
  // WriteUnpreparedTxn::flushed_save_points_ holds information about savepoints
  // set on unprepared batches that have already flushed. It holds the snapshot
  // and unprep_seqs at that savepoint, so that the rollback process can
  // determine which keys were visible at that point in time.
  //
  // WriteUnpreparecTxn::unflushed_save_points_ holds information about
  // savepoints on the current in-memory write_batch_. It simply records the
  // size of the write batch at every savepoint.
  //
  // TODO(lth): Remove the redundancy between save_point_boundaries_ and
  // write_batch_.save_points_.
  //
  // Based on this information, here are some invariants:
  // size(unflushed_save_points_) = size(write_batch_.save_points_)
  // size(flushed_save_points_) + size(unflushed_save_points_)
  //   = size(save_points_)
  //
  std::unique_ptr<autovector<WriteUnpreparedTxn::SavePoint>>
      flushed_save_points_;
  std::unique_ptr<autovector<size_t>> unflushed_save_points_;

  // It is currently unsafe to flush a write batch if there are active iterators
  // created from this transaction. This is because we use WriteBatchWithIndex
  // to do merging reads from the DB and the write batch. If we flush the write
  // batch, it is possible that the delta iterator on the iterator will point to
  // invalid memory.
  std::vector<Iterator*> active_iterators_;

  // Untracked keys that we have to rollback.
  //
  // TODO(lth): Currently we we do not record untracked keys per-savepoint.
  // This means that when rolling back to savepoints, we have to check all
  // keys in the current transaction for rollback. Note that this is only
  // inefficient, but still correct because we take a snapshot at every
  // savepoint, and we will use that snapshot to construct the rollback batch.
  // The rollback batch will then contain a reissue of the same marker.
  //
  // A more optimal solution would be to only check keys changed since the
  // last savepoint. Also, it may make sense to merge this into tracked_keys_
  // and differentiate between tracked but not locked keys to avoid having two
  // very similar data structures.
  using KeySet = std::unordered_map<uint32_t, std::vector<std::string>>;
  KeySet untracked_keys_;
};

}  // namespace ROCKSDB_NAMESPACE
