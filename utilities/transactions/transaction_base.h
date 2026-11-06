// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stack>
#include <string>
#include <unordered_set>
#include <vector>

#include "db/write_batch_internal.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "util/autovector.h"
#include "utilities/transactions/lock/lock_tracker.h"
#include "utilities/transactions/transaction_util.h"

namespace ROCKSDB_NAMESPACE {

class TransactionBaseImpl : public Transaction {
 public:
  TransactionBaseImpl(DB* db, const WriteOptions& write_options,
                      const LockTrackerFactory& lock_tracker_factory);

  ~TransactionBaseImpl() override;

  // Remove pending operations queued in this transaction.
  virtual void Clear();

  void Reinitialize(DB* db, const WriteOptions& write_options);

  // Called before executing Put, PutEntity, Merge, Delete, and GetForUpdate. If
  // TryLock returns non-OK, the Put/PutEntity/Merge/Delete/GetForUpdate will be
  // failed. do_validate will be false if called from PutUntracked,
  // PutEntityUntracked, DeleteUntracked, MergeUntracked, or
  // GetForUpdate(do_validate=false)
  virtual Status TryLock(ColumnFamilyHandle* column_family, const Slice& key,
                         bool read_only, bool exclusive,
                         const bool do_validate = true,
                         const bool assume_tracked = false) = 0;

  void SetSavePoint() override;

  Status RollbackToSavePoint() override;

  Status PopSavePoint() override;

  using Transaction::Get;
  Status Get(const ReadOptions& _read_options,
             ColumnFamilyHandle* column_family, const Slice& key,
             std::string* value) override;

  Status Get(const ReadOptions& _read_options,
             ColumnFamilyHandle* column_family, const Slice& key,
             PinnableSlice* value) override;

  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override {
    return Get(options, db_->DefaultColumnFamily(), key, value);
  }

  Status GetEntity(const ReadOptions& options,
                   ColumnFamilyHandle* column_family, const Slice& key,
                   PinnableWideColumns* columns) override;

  using Transaction::GetForUpdate;
  Status GetForUpdate(const ReadOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      std::string* value, bool exclusive,
                      const bool do_validate) override;

  Status GetForUpdate(const ReadOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      PinnableSlice* pinnable_val, bool exclusive,
                      const bool do_validate) override;

  Status GetForUpdate(const ReadOptions& options, const Slice& key,
                      std::string* value, bool exclusive,
                      const bool do_validate) override {
    return GetForUpdate(options, db_->DefaultColumnFamily(), key, value,
                        exclusive, do_validate);
  }

  Status GetForUpdate(const ReadOptions& options, const Slice& key,
                      PinnableSlice* pinnable_val, bool exclusive,
                      const bool do_validate) override {
    return GetForUpdate(options, db_->DefaultColumnFamily(), key, pinnable_val,
                        exclusive, do_validate);
  }

  Status GetEntityForUpdate(const ReadOptions& read_options,
                            ColumnFamilyHandle* column_family, const Slice& key,
                            PinnableWideColumns* columns, bool exclusive = true,
                            bool do_validate = true) override;

  using Transaction::MultiGet;
  std::vector<Status> MultiGet(
      const ReadOptions& _read_options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override;

  std::vector<Status> MultiGet(const ReadOptions& options,
                               const std::vector<Slice>& keys,
                               std::vector<std::string>* values) override {
    return MultiGet(options,
                    std::vector<ColumnFamilyHandle*>(
                        keys.size(), db_->DefaultColumnFamily()),
                    keys, values);
  }

  void MultiGet(const ReadOptions& _read_options,
                ColumnFamilyHandle* column_family, const size_t num_keys,
                const Slice* keys, PinnableSlice* values, Status* statuses,
                const bool sorted_input = false) override;

  void MultiGetEntity(const ReadOptions& options,
                      ColumnFamilyHandle* column_family, size_t num_keys,
                      const Slice* keys, PinnableWideColumns* results,
                      Status* statuses, bool sorted_input = false) override;

  using Transaction::MultiGetForUpdate;
  std::vector<Status> MultiGetForUpdate(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override;

  std::vector<Status> MultiGetForUpdate(
      const ReadOptions& options, const std::vector<Slice>& keys,
      std::vector<std::string>* values) override {
    return MultiGetForUpdate(options,
                             std::vector<ColumnFamilyHandle*>(
                                 keys.size(), db_->DefaultColumnFamily()),
                             keys, values);
  }

  Iterator* GetIterator(const ReadOptions& read_options) override;
  Iterator* GetIterator(const ReadOptions& read_options,
                        ColumnFamilyHandle* column_family) override;

  std::unique_ptr<Iterator> GetCoalescingIterator(
      const ReadOptions& read_options,
      const std::vector<ColumnFamilyHandle*>& column_families) override;

  std::unique_ptr<AttributeGroupIterator> GetAttributeGroupIterator(
      const ReadOptions& read_options,
      const std::vector<ColumnFamilyHandle*>& column_families) override;

  Status Put(ColumnFamilyHandle* column_family, const Slice& key,
             const Slice& value, const bool assume_tracked = false) override;
  Status Put(const Slice& key, const Slice& value) override {
    return Put(nullptr, key, value);
  }

  Status Put(ColumnFamilyHandle* column_family, const SliceParts& key,
             const SliceParts& value,
             const bool assume_tracked = false) override;
  Status Put(const SliceParts& key, const SliceParts& value) override {
    return Put(nullptr, key, value);
  }

  Status PutEntity(ColumnFamilyHandle* column_family, const Slice& key,
                   const WideColumns& columns,
                   bool assume_tracked = false) override {
    const bool do_validate = !assume_tracked;

    return PutEntityImpl(column_family, key, columns, do_validate,
                         assume_tracked);
  }

  Status Merge(ColumnFamilyHandle* column_family, const Slice& key,
               const Slice& value, const bool assume_tracked = false) override;
  Status Merge(const Slice& key, const Slice& value) override {
    return Merge(nullptr, key, value);
  }

  Status Delete(ColumnFamilyHandle* column_family, const Slice& key,
                const bool assume_tracked = false) override;
  Status Delete(const Slice& key) override { return Delete(nullptr, key); }
  Status Delete(ColumnFamilyHandle* column_family, const SliceParts& key,
                const bool assume_tracked = false) override;
  Status Delete(const SliceParts& key) override { return Delete(nullptr, key); }

  Status SingleDelete(ColumnFamilyHandle* column_family, const Slice& key,
                      const bool assume_tracked = false) override;
  Status SingleDelete(const Slice& key) override {
    return SingleDelete(nullptr, key);
  }
  Status SingleDelete(ColumnFamilyHandle* column_family, const SliceParts& key,
                      const bool assume_tracked = false) override;
  Status SingleDelete(const SliceParts& key) override {
    return SingleDelete(nullptr, key);
  }

  Status PutUntracked(ColumnFamilyHandle* column_family, const Slice& key,
                      const Slice& value) override;
  Status PutUntracked(const Slice& key, const Slice& value) override {
    return PutUntracked(nullptr, key, value);
  }

  Status PutUntracked(ColumnFamilyHandle* column_family, const SliceParts& key,
                      const SliceParts& value) override;
  Status PutUntracked(const SliceParts& key, const SliceParts& value) override {
    return PutUntracked(nullptr, key, value);
  }

  Status PutEntityUntracked(ColumnFamilyHandle* column_family, const Slice& key,
                            const WideColumns& columns) override {
    constexpr bool do_validate = false;
    constexpr bool assume_tracked = false;

    return PutEntityImpl(column_family, key, columns, do_validate,
                         assume_tracked);
  }

  Status MergeUntracked(ColumnFamilyHandle* column_family, const Slice& key,
                        const Slice& value) override;
  Status MergeUntracked(const Slice& key, const Slice& value) override {
    return MergeUntracked(nullptr, key, value);
  }

  Status DeleteUntracked(ColumnFamilyHandle* column_family,
                         const Slice& key) override;
  Status DeleteUntracked(const Slice& key) override {
    return DeleteUntracked(nullptr, key);
  }
  Status DeleteUntracked(ColumnFamilyHandle* column_family,
                         const SliceParts& key) override;
  Status DeleteUntracked(const SliceParts& key) override {
    return DeleteUntracked(nullptr, key);
  }

  Status SingleDeleteUntracked(ColumnFamilyHandle* column_family,
                               const Slice& key) override;
  Status SingleDeleteUntracked(const Slice& key) override {
    return SingleDeleteUntracked(nullptr, key);
  }

  void PutLogData(const Slice& blob) override;

  WriteBatchWithIndex* GetWriteBatch() override;

  void SetLockTimeout(int64_t /*timeout*/) override { /* Do nothing */ }

  void SetDeadlockTimeout(int64_t /*timeout*/) override { /* Do nothing */ }

  const Snapshot* GetSnapshot() const override {
    // will return nullptr when there is no snapshot
    return snapshot_.get();
  }

  std::shared_ptr<const Snapshot> GetTimestampedSnapshot() const override {
    return snapshot_;
  }

  void SetSnapshot() override;
  void SetSnapshotOnNextOperation(
      std::shared_ptr<TransactionNotifier> notifier = nullptr) override;

  void ClearSnapshot() override {
    snapshot_.reset();
    snapshot_needed_ = false;
    snapshot_notifier_ = nullptr;
  }

  void DisableIndexing() override { indexing_enabled_ = false; }

  void EnableIndexing() override { indexing_enabled_ = true; }

  bool IndexingEnabled() const { return indexing_enabled_; }

  uint64_t GetElapsedTime() const override;

  uint64_t GetNumPuts() const override;

  uint64_t GetNumPutEntities() const override;

  uint64_t GetNumDeletes() const override;

  uint64_t GetNumMerges() const override;

  uint64_t GetNumKeys() const override;

  void UndoGetForUpdate(ColumnFamilyHandle* column_family,
                        const Slice& key) override;
  void UndoGetForUpdate(const Slice& key) override {
    return UndoGetForUpdate(nullptr, key);
  }

  WriteOptions* GetWriteOptions() override { return &write_options_; }

  void SetWriteOptions(const WriteOptions& write_options) override {
    write_options_ = write_options;
  }

  // Used for memory management for snapshot_
  void ReleaseSnapshot(const Snapshot* snapshot, DB* db);

  // iterates over the given batch and makes the appropriate inserts.
  // used for rebuilding prepared transactions after recovery.
  Status RebuildFromWriteBatch(WriteBatch* src_batch) override;

  WriteBatch* GetCommitTimeWriteBatch() override;

  LockTracker& GetTrackedLocks() { return *tracked_locks_; }

 protected:
  ColumnFamilyHandle* DefaultColumnFamily() const {
    assert(db_);
    return db_->DefaultColumnFamily();
  }

  template <typename IterType, typename ImplType,
            typename ErrorIteratorFuncType>
  std::unique_ptr<IterType> NewMultiCfIterator(
      const ReadOptions& read_options,
      const std::vector<ColumnFamilyHandle*>& column_families,
      ErrorIteratorFuncType error_iterator_func);

  Status GetImpl(const ReadOptions& options, ColumnFamilyHandle* column_family,
                 const Slice& key, std::string* value) override;

  Status GetImpl(const ReadOptions& options, ColumnFamilyHandle* column_family,
                 const Slice& key, PinnableSlice* value) override;

  // Transaction policies with custom visibility rules override these hooks to
  // supply their ReadCallback while reusing the public entity-read plumbing.
  virtual Status GetEntityImpl(const ReadOptions& options,
                               ColumnFamilyHandle* column_family,
                               const Slice& key, PinnableWideColumns* columns) {
    return write_batch_.GetEntityFromBatchAndDB(db_, options, column_family,
                                                key, columns);
  }

  virtual void MultiGetEntityImpl(const ReadOptions& options,
                                  ColumnFamilyHandle* column_family,
                                  size_t num_keys, const Slice* keys,
                                  PinnableWideColumns* results,
                                  Status* statuses, bool sorted_input) {
    write_batch_.MultiGetEntityFromBatchAndDB(db_, options, column_family,
                                              num_keys, keys, results, statuses,
                                              sorted_input);
  }

  Status PutEntityImpl(ColumnFamilyHandle* column_family, const Slice& key,
                       const WideColumns& columns, bool do_validate,
                       bool assume_tracked);

  // Add a key to the list of tracked keys.
  //
  // seqno is the earliest seqno this key was involved with this transaction.
  // readonly should be set to true if no data was written for this key
  void TrackKey(uint32_t cfh_id, const std::string& key, SequenceNumber seqno,
                bool readonly, bool exclusive);

  // Called when UndoGetForUpdate determines that this key can be unlocked.
  virtual void UnlockGetForUpdate(ColumnFamilyHandle* column_family,
                                  const Slice& key) = 0;

  // Sets a snapshot if SetSnapshotOnNextOperation() has been called.
  void SetSnapshotIfNeeded();

  // Initialize write_batch_ for 2PC by inserting Noop.
  inline void InitWriteBatch(bool clear = false) {
    if (clear) {
      write_batch_.Clear();
    }
    assert(write_batch_.GetDataSize() == WriteBatchInternal::kHeader);
    auto s = WriteBatchInternal::InsertNoop(write_batch_.GetWriteBatch());
    assert(s.ok());
  }

  WriteBatchBase* GetBatchForWrite();

  DB* db_;
  DBImpl* dbimpl_;

  WriteOptions write_options_;

  const Comparator* cmp_;

  const LockTrackerFactory& lock_tracker_factory_;

  // Stores that time the txn was constructed, in microseconds.
  uint64_t start_time_;

  // Stores the current snapshot that was set by SetSnapshot or null if
  // no snapshot is currently set.
  std::shared_ptr<const Snapshot> snapshot_;

  // Count of various operations pending in this transaction
  uint64_t num_puts_ = 0;
  uint64_t num_put_entities_ = 0;
  uint64_t num_deletes_ = 0;
  uint64_t num_merges_ = 0;

  struct SavePoint {
    std::shared_ptr<const Snapshot> snapshot_;
    bool snapshot_needed_ = false;
    std::shared_ptr<TransactionNotifier> snapshot_notifier_;
    uint64_t num_puts_ = 0;
    uint64_t num_put_entities_ = 0;
    uint64_t num_deletes_ = 0;
    uint64_t num_merges_ = 0;

    // Record all locks tracked since the last savepoint
    std::shared_ptr<LockTracker> new_locks_;

    SavePoint(std::shared_ptr<const Snapshot> snapshot, bool snapshot_needed,
              std::shared_ptr<TransactionNotifier> snapshot_notifier,
              uint64_t num_puts, uint64_t num_put_entities,
              uint64_t num_deletes, uint64_t num_merges,
              const LockTrackerFactory& lock_tracker_factory)
        : snapshot_(snapshot),
          snapshot_needed_(snapshot_needed),
          snapshot_notifier_(snapshot_notifier),
          num_puts_(num_puts),
          num_put_entities_(num_put_entities),
          num_deletes_(num_deletes),
          num_merges_(num_merges),
          new_locks_(lock_tracker_factory.Create()) {}

    explicit SavePoint(const LockTrackerFactory& lock_tracker_factory)
        : new_locks_(lock_tracker_factory.Create()) {}
  };

  // Records writes pending in this transaction
  WriteBatchWithIndex write_batch_;

  // For Pessimistic Transactions this is the set of acquired locks.
  // Optimistic Transactions will keep note the requested locks (not actually
  // locked), and do conflict checking until commit time based on the tracked
  // lock requests.
  std::unique_ptr<LockTracker> tracked_locks_;

  // Stack of the Snapshot saved at each save point. Saved snapshots may be
  // nullptr if there was no snapshot at the time SetSavePoint() was called.
  std::unique_ptr<std::stack<TransactionBaseImpl::SavePoint,
                             autovector<TransactionBaseImpl::SavePoint>>>
      save_points_;

 private:
  friend class WriteCommittedTxn;
  friend class WritePreparedTxn;

  // Extra data to be persisted with the commit. Note this is only used when
  // prepare phase is not skipped.
  WriteBatch commit_time_batch_;

  // Column families that no longer existed when this transaction was rebuilt
  // from the WAL (see RebuildFromWriteBatch). Their writes are kept in
  // write_batch_ with their original column family id but are not indexed
  // and have no comparator here.
  std::unordered_set<uint32_t> dropped_cfs_in_rebuilt_batch_;

  // If true, future Put/PutEntity/Merge/Delete operations will be indexed in
  // the WriteBatchWithIndex. If false, future Put/PutEntity/Merge/Delete
  // operations will be inserted directly into the underlying WriteBatch and not
  // indexed in the WriteBatchWithIndex.
  bool indexing_enabled_;

  // SetSnapshotOnNextOperation() has been called and the snapshot has not yet
  // been reset.
  bool snapshot_needed_ = false;

  // SetSnapshotOnNextOperation() has been called and the caller would like
  // a notification through the TransactionNotifier interface
  std::shared_ptr<TransactionNotifier> snapshot_notifier_ = nullptr;

  Status TryLock(ColumnFamilyHandle* column_family, const SliceParts& key,
                 bool read_only, bool exclusive, const bool do_validate = true,
                 const bool assume_tracked = false);

  void SetSnapshotInternal(const Snapshot* snapshot);
};

}  // namespace ROCKSDB_NAMESPACE
