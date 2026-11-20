// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <atomic>
#include <mutex>
#include <stack>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/write_callback.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "util/autovector.h"
#include "utilities/transactions/transaction_base.h"
#include "utilities/transactions/transaction_util.h"

namespace ROCKSDB_NAMESPACE {

class PessimisticTransactionDB;

// A transaction under pessimistic concurrency control. This class implements
// the locking API and interfaces with the lock manager as well as the
// pessimistic transactional db.
class PessimisticTransaction : public TransactionBaseImpl {
 public:
  PessimisticTransaction(TransactionDB* db, const WriteOptions& write_options,
                         const TransactionOptions& txn_options,
                         const bool init = true);
  // No copying allowed
  PessimisticTransaction(const PessimisticTransaction&) = delete;
  void operator=(const PessimisticTransaction&) = delete;
  PessimisticTransaction(PessimisticTransaction&&) = delete;
  void operator=(PessimisticTransaction&&) = delete;

  ~PessimisticTransaction() override;

  void Reinitialize(TransactionDB* txn_db, const WriteOptions& write_options,
                    const TransactionOptions& txn_options);

  Status Prepare() override;

  Status Commit() override;

  // It is basically Commit without going through Prepare phase. The write batch
  // is also directly provided instead of expecting txn to gradually batch the
  // transactions writes to an internal write batch.
  Status CommitBatch(WriteBatch* batch);

  Status Rollback() override;

  Status RollbackToSavePoint() override;

  Status SetName(const TransactionName& name) override;

  // Generate a new unique transaction identifier
  static TransactionID GenTxnID();

  TransactionID GetID() const override { return txn_id_; }

  std::vector<TransactionID> GetWaitingTxns(uint32_t* column_family_id,
                                            std::string* key) const override {
    std::lock_guard<std::mutex> lock(wait_mutex_);
    std::vector<TransactionID> ids(waiting_txn_ids_.size());
    if (timed_out_key_.has_value()) {
      if (key) {
        *key = timed_out_key_.value();
      }
    } else {
      if (key) {
        *key = waiting_key_ ? *waiting_key_ : "";
      }
    }
    if (column_family_id) {
      *column_family_id = waiting_cf_id_;
    }
    std::copy(waiting_txn_ids_.begin(), waiting_txn_ids_.end(), ids.begin());
    return ids;
  }

  void SetWaitingTxn(autovector<TransactionID>& ids, uint32_t column_family_id,
                     const std::string* key, bool is_timed_out = false) {
    std::lock_guard<std::mutex> lock(wait_mutex_);
    waiting_txn_ids_ = ids;
    waiting_cf_id_ = column_family_id;
    if (is_timed_out) {
      timed_out_key_ = key ? *key : "";
    } else {
      waiting_key_ = key;
    }
  }

  void ClearWaitingTxn() {
    std::lock_guard<std::mutex> lock(wait_mutex_);
    waiting_txn_ids_.clear();
    waiting_cf_id_ = 0;
    waiting_key_ = nullptr;
  }

  // Returns the time (in microseconds according to Env->GetMicros())
  // that this transaction will be expired.  Returns 0 if this transaction does
  // not expire.
  uint64_t GetExpirationTime() const { return expiration_time_; }

  // returns true if this transaction has an expiration_time and has expired.
  bool IsExpired() const;

  // Returns the number of microseconds a transaction can wait on acquiring a
  // lock or -1 if there is no timeout.
  int64_t GetLockTimeout() const { return lock_timeout_; }
  void SetLockTimeout(int64_t timeout) override {
    lock_timeout_ = timeout * 1000;
  }
  int64_t GetDeadlockTimeout() const { return deadlock_timeout_us_; }
  void SetDeadlockTimeout(int64_t timeout_ms) override {
    deadlock_timeout_us_ = timeout_ms * 1000;
  }

  // Returns true if locks were stolen successfully, false otherwise.
  bool TryStealingLocks();

  bool IsDeadlockDetect() const override { return deadlock_detect_; }

  int64_t GetDeadlockDetectDepth() const { return deadlock_detect_depth_; }

  Status GetRangeLock(ColumnFamilyHandle* column_family,
                      const Endpoint& start_key,
                      const Endpoint& end_key) override;

  Status CollapseKey(const ReadOptions& options, const Slice& key,
                     ColumnFamilyHandle* column_family = nullptr) override;

 protected:
  virtual Status PrepareInternal() = 0;

  virtual Status CommitWithoutPrepareInternal() = 0;

  // batch_cnt if non-zero is the number of sub-batches. A sub-batch is a batch
  // with no duplicate keys. If zero, then the number of sub-batches is unknown.
  virtual Status CommitBatchInternal(WriteBatch* batch,
                                     size_t batch_cnt = 0) = 0;

  virtual Status CommitInternal() = 0;

  virtual Status RollbackInternal() = 0;

  virtual void Initialize(const TransactionOptions& txn_options);

  Status LockBatch(WriteBatch* batch, LockTracker* keys_to_unlock);

  Status TryLock(ColumnFamilyHandle* column_family, const Slice& key,
                 bool read_only, bool exclusive, const bool do_validate = true,
                 const bool assume_tracked = false) override;

  void Clear() override;

  PessimisticTransactionDB* txn_db_impl_;
  DBImpl* db_impl_;

  // If non-zero, this transaction should not be committed after this time (in
  // microseconds according to Env->NowMicros())
  uint64_t expiration_time_;

  // Timestamp used by the transaction to perform all GetForUpdate.
  // Use this timestamp for conflict checking.
  // read_timestamp_ == kMaxTxnTimestamp means this transaction has not
  // performed any GetForUpdate. It is possible that the transaction has
  // performed blind writes or Get, though.
  TxnTimestamp read_timestamp_{kMaxTxnTimestamp};
  TxnTimestamp commit_timestamp_{kMaxTxnTimestamp};

  // Refer to
  // TransactionOptions::use_only_the_last_commit_time_batch_for_recovery
  bool use_only_the_last_commit_time_batch_for_recovery_ = false;
  // Refer to
  // TransactionOptions::skip_prepare
  bool skip_prepare_ = false;
  // Refer to TransactionOptions::commit_bypass_memtable
  uint32_t commit_bypass_memtable_threshold_ =
      std::numeric_limits<uint32_t>::max();
  uint64_t commit_bypass_memtable_byte_threshold_ =
      std::numeric_limits<uint64_t>::max();

 private:
  friend class TransactionTest_ValidateSnapshotTest_Test;
  // Used to create unique ids for transactions.
  static std::atomic<TransactionID> txn_id_counter_;

  // Unique ID for this transaction
  TransactionID txn_id_;

  // IDs for the transactions that are blocking the current transaction.
  //
  // empty if current transaction is not waiting or has timed out
  autovector<TransactionID> waiting_txn_ids_;

  // The following two represents the (cf, key) that a transaction is waiting
  // on.
  //
  // If waiting_key_ is not null, then the pointer should always point to
  // a valid string object. The reason is that it is only non-null when the
  // transaction is blocked in the PointLockManager::AcquireWithTimeout
  // function. At that point, the key string object is one of the function
  // parameters.
  uint32_t waiting_cf_id_;
  const std::string* waiting_key_;

  // Waiting key with lifetime of the txn so it can be accessed after timeouts
  std::optional<std::string> timed_out_key_;

  // Mutex protecting waiting_txn_ids_, waiting_cf_id_ and waiting_key_.
  mutable std::mutex wait_mutex_;

  // Timeout in microseconds when locking a key or -1 if there is no timeout.
  int64_t lock_timeout_;

  // Timeout in microseconds before perform dead lock detection.
  // If 0, deadlock detection will be performed immediately.
  int64_t deadlock_timeout_us_;

  // Whether to perform deadlock detection or not.
  bool deadlock_detect_;

  // Whether to perform deadlock detection or not.
  int64_t deadlock_detect_depth_;

  // Refer to TransactionOptions::skip_concurrency_control
  bool skip_concurrency_control_;

  virtual Status ValidateSnapshot(ColumnFamilyHandle* column_family,
                                  const Slice& key,
                                  SequenceNumber* tracked_at_seq);

  void UnlockGetForUpdate(ColumnFamilyHandle* column_family,
                          const Slice& key) override;
};

class WriteCommittedTxn : public PessimisticTransaction {
 public:
  WriteCommittedTxn(TransactionDB* db, const WriteOptions& write_options,
                    const TransactionOptions& txn_options);
  // No copying allowed
  WriteCommittedTxn(const WriteCommittedTxn&) = delete;
  void operator=(const WriteCommittedTxn&) = delete;
  WriteCommittedTxn(WriteCommittedTxn&&) = delete;
  void operator=(WriteCommittedTxn&&) = delete;

  ~WriteCommittedTxn() override {}

  using TransactionBaseImpl::GetForUpdate;
  Status GetForUpdate(const ReadOptions& read_options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      std::string* value, bool exclusive,
                      const bool do_validate) override;
  Status GetForUpdate(const ReadOptions& read_options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      PinnableSlice* pinnable_val, bool exclusive,
                      const bool do_validate) override;

  Status GetEntityForUpdate(const ReadOptions& read_options,
                            ColumnFamilyHandle* column_family, const Slice& key,
                            PinnableWideColumns* columns, bool exclusive,
                            bool do_validate) override;

  using TransactionBaseImpl::Put;
  // `key` does NOT include timestamp even when it's enabled.
  Status Put(ColumnFamilyHandle* column_family, const Slice& key,
             const Slice& value, const bool assume_tracked = false) override;
  Status Put(ColumnFamilyHandle* column_family, const SliceParts& key,
             const SliceParts& value,
             const bool assume_tracked = false) override;

  using TransactionBaseImpl::PutUntracked;
  Status PutUntracked(ColumnFamilyHandle* column_family, const Slice& key,
                      const Slice& value) override;
  Status PutUntracked(ColumnFamilyHandle* column_family, const SliceParts& key,
                      const SliceParts& value) override;

  // `key` does NOT include timestamp even when it's enabled.
  Status PutEntity(ColumnFamilyHandle* column_family, const Slice& key,
                   const WideColumns& columns,
                   bool assume_tracked = false) override {
    const bool do_validate = !assume_tracked;

    return PutEntityImpl(column_family, key, columns, do_validate,
                         assume_tracked);
  }

  Status PutEntityUntracked(ColumnFamilyHandle* column_family, const Slice& key,
                            const WideColumns& columns) override {
    constexpr bool do_validate = false;
    constexpr bool assume_tracked = false;

    return PutEntityImpl(column_family, key, columns, do_validate,
                         assume_tracked);
  }

  using TransactionBaseImpl::Delete;
  // `key` does NOT include timestamp even when it's enabled.
  Status Delete(ColumnFamilyHandle* column_family, const Slice& key,
                const bool assume_tracked = false) override;
  Status Delete(ColumnFamilyHandle* column_family, const SliceParts& key,
                const bool assume_tracked = false) override;

  using TransactionBaseImpl::DeleteUntracked;
  Status DeleteUntracked(ColumnFamilyHandle* column_family,
                         const Slice& key) override;
  Status DeleteUntracked(ColumnFamilyHandle* column_family,
                         const SliceParts& key) override;

  using TransactionBaseImpl::SingleDelete;
  // `key` does NOT include timestamp even when it's enabled.
  Status SingleDelete(ColumnFamilyHandle* column_family, const Slice& key,
                      const bool assume_tracked = false) override;
  Status SingleDelete(ColumnFamilyHandle* column_family, const SliceParts& key,
                      const bool assume_tracked = false) override;

  using TransactionBaseImpl::SingleDeleteUntracked;
  Status SingleDeleteUntracked(ColumnFamilyHandle* column_family,
                               const Slice& key) override;

  using TransactionBaseImpl::Merge;
  Status Merge(ColumnFamilyHandle* column_family, const Slice& key,
               const Slice& value, const bool assume_tracked = false) override;

  Status SetReadTimestampForValidation(TxnTimestamp ts) override;
  Status SetCommitTimestamp(TxnTimestamp ts) override;
  TxnTimestamp GetCommitTimestamp() const override { return commit_timestamp_; }

 private:
  template <typename TValue>
  Status GetForUpdateImpl(const ReadOptions& read_options,
                          ColumnFamilyHandle* column_family, const Slice& key,
                          TValue* value, bool exclusive,
                          const bool do_validate);

  Status PutEntityImpl(ColumnFamilyHandle* column_family, const Slice& key,
                       const WideColumns& columns, bool do_validate,
                       bool assume_tracked);

  template <typename TKey, typename TOperation>
  Status Operate(ColumnFamilyHandle* column_family, const TKey& key,
                 const bool do_validate, const bool assume_tracked,
                 TOperation&& operation);

  Status PrepareInternal() override;

  Status CommitWithoutPrepareInternal() override;

  Status CommitBatchInternal(WriteBatch* batch, size_t batch_cnt) override;

  Status CommitInternal() override;

  Status RollbackInternal() override;

  // Checks if the combination of `do_validate`, the read timestamp set in
  // `read_timestamp_` and the `enable_udt_validation` flag in
  // TransactionDBOptions make sense together.
  Status SanityCheckReadTimestamp(bool do_validate);

  // Column families that enable timestamps and whose data are written when
  // indexing_enabled_ is false. If a key is written when indexing_enabled_ is
  // true, then the corresponding column family is not added to cfs_with_ts
  // even if it enables timestamp.
  std::unordered_set<uint32_t> cfs_with_ts_tracked_when_indexing_disabled_;
};

}  // namespace ROCKSDB_NAMESPACE
