//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/transactions/pessimistic_transaction.h"

#include <map>
#include <set>
#include <string>
#include <vector>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "logging/logging.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/string_util.h"
#include "utilities/transactions/pessimistic_transaction_db.h"
#include "utilities/transactions/transaction_util.h"
#include "utilities/write_batch_with_index/write_batch_with_index_internal.h"

namespace ROCKSDB_NAMESPACE {

struct WriteOptions;

std::atomic<TransactionID> PessimisticTransaction::txn_id_counter_(1);

TransactionID PessimisticTransaction::GenTxnID() {
  return txn_id_counter_.fetch_add(1);
}

PessimisticTransaction::PessimisticTransaction(
    TransactionDB* txn_db, const WriteOptions& write_options,
    const TransactionOptions& txn_options, const bool init)
    : TransactionBaseImpl(
          txn_db->GetRootDB(), write_options,
          static_cast_with_check<PessimisticTransactionDB>(txn_db)
              ->GetLockTrackerFactory()),
      txn_db_impl_(nullptr),
      expiration_time_(0),
      txn_id_(0),
      waiting_cf_id_(0),
      waiting_key_(nullptr),
      lock_timeout_(0),
      deadlock_detect_(false),
      deadlock_detect_depth_(0),
      skip_concurrency_control_(false) {
  txn_db_impl_ = static_cast_with_check<PessimisticTransactionDB>(txn_db);
  db_impl_ = static_cast_with_check<DBImpl>(db_);
  if (init) {
    PessimisticTransaction::Initialize(txn_options);
  }
}

void PessimisticTransaction::Initialize(const TransactionOptions& txn_options) {
  // Range lock manager uses address of transaction object as TXNID
  const TransactionDBOptions& db_options = txn_db_impl_->GetTxnDBOptions();
  if (db_options.lock_mgr_handle &&
      db_options.lock_mgr_handle->getLockManager()->IsRangeLockSupported()) {
    txn_id_ = reinterpret_cast<TransactionID>(this);
  } else {
    txn_id_ = GenTxnID();
  }

  txn_state_ = STARTED;

  deadlock_detect_ = txn_options.deadlock_detect;
  deadlock_detect_depth_ = txn_options.deadlock_detect_depth;
  write_batch_.SetMaxBytes(txn_options.max_write_batch_size);
  write_batch_.GetWriteBatch()->SetTrackTimestampSize(
      txn_options.write_batch_track_timestamp_size);
  skip_concurrency_control_ = txn_options.skip_concurrency_control;

  lock_timeout_ = txn_options.lock_timeout * 1000;
  if (lock_timeout_ < 0) {
    // Lock timeout not set, use default
    lock_timeout_ =
        txn_db_impl_->GetTxnDBOptions().transaction_lock_timeout * 1000;
  }

  // deadlock timeout should be lower than lock timeout
  deadlock_timeout_us_ =
      std::min(txn_options.deadlock_timeout_us, lock_timeout_);

  if (txn_options.expiration >= 0) {
    expiration_time_ = start_time_ + txn_options.expiration * 1000;
  } else {
    expiration_time_ = 0;
  }

  if (txn_options.set_snapshot) {
    SetSnapshot();
  }

  if (expiration_time_ > 0) {
    txn_db_impl_->InsertExpirableTransaction(txn_id_, this);
  }
  use_only_the_last_commit_time_batch_for_recovery_ =
      txn_options.use_only_the_last_commit_time_batch_for_recovery;
  skip_prepare_ = txn_options.skip_prepare;

  read_timestamp_ = kMaxTxnTimestamp;
  commit_timestamp_ = kMaxTxnTimestamp;

  if (txn_options.commit_bypass_memtable) {
    // No need to optimize for empty transction
    commit_bypass_memtable_threshold_ = 1;
  } else {
    commit_bypass_memtable_threshold_ =
        txn_options.large_txn_commit_optimize_threshold;
  }

  commit_bypass_memtable_byte_threshold_ =
      txn_options.large_txn_commit_optimize_byte_threshold;
}

PessimisticTransaction::~PessimisticTransaction() {
  txn_db_impl_->UnLock(this, *tracked_locks_);
  if (expiration_time_ > 0) {
    txn_db_impl_->RemoveExpirableTransaction(txn_id_);
  }
  if (!name_.empty() && txn_state_ != COMMITTED) {
    txn_db_impl_->UnregisterTransaction(this);
  }
}

void PessimisticTransaction::Clear() {
  txn_db_impl_->UnLock(this, *tracked_locks_);
  TransactionBaseImpl::Clear();
}

void PessimisticTransaction::Reinitialize(
    TransactionDB* txn_db, const WriteOptions& write_options,
    const TransactionOptions& txn_options) {
  if (!name_.empty() && txn_state_ != COMMITTED) {
    txn_db_impl_->UnregisterTransaction(this);
  }
  TransactionBaseImpl::Reinitialize(txn_db->GetRootDB(), write_options);
  Initialize(txn_options);
}

bool PessimisticTransaction::IsExpired() const {
  if (expiration_time_ > 0) {
    if (dbimpl_->GetSystemClock()->NowMicros() >= expiration_time_) {
      // Transaction is expired.
      return true;
    }
  }

  return false;
}

WriteCommittedTxn::WriteCommittedTxn(TransactionDB* txn_db,
                                     const WriteOptions& write_options,
                                     const TransactionOptions& txn_options)
    : PessimisticTransaction(txn_db, write_options, txn_options) {}

Status WriteCommittedTxn::GetForUpdate(const ReadOptions& read_options,
                                       ColumnFamilyHandle* column_family,
                                       const Slice& key, std::string* value,
                                       bool exclusive, const bool do_validate) {
  return GetForUpdateImpl(read_options, column_family, key, value, exclusive,
                          do_validate);
}

Status WriteCommittedTxn::GetForUpdate(const ReadOptions& read_options,
                                       ColumnFamilyHandle* column_family,
                                       const Slice& key,
                                       PinnableSlice* pinnable_val,
                                       bool exclusive, const bool do_validate) {
  return GetForUpdateImpl(read_options, column_family, key, pinnable_val,
                          exclusive, do_validate);
}

template <typename TValue>
inline Status WriteCommittedTxn::GetForUpdateImpl(
    const ReadOptions& read_options, ColumnFamilyHandle* column_family,
    const Slice& key, TValue* value, bool exclusive, const bool do_validate) {
  if (read_options.io_activity != Env::IOActivity::kUnknown) {
    return Status::InvalidArgument(
        "Cannot call GetForUpdate with `ReadOptions::io_activity` != "
        "`Env::IOActivity::kUnknown`");
  }
  column_family =
      column_family ? column_family : db_impl_->DefaultColumnFamily();
  assert(column_family);
  if (!read_options.timestamp) {
    const Comparator* const ucmp = column_family->GetComparator();
    assert(ucmp);
    size_t ts_sz = ucmp->timestamp_size();
    if (0 == ts_sz) {
      return TransactionBaseImpl::GetForUpdate(read_options, column_family, key,
                                               value, exclusive, do_validate);
    }
  } else {
    Status s =
        db_impl_->FailIfTsMismatchCf(column_family, *(read_options.timestamp));
    if (!s.ok()) {
      return s;
    }
  }

  Status s = SanityCheckReadTimestamp(do_validate);
  if (!s.ok()) {
    return s;
  }

  if (!read_options.timestamp) {
    ReadOptions read_opts_copy = read_options;
    char ts_buf[sizeof(kMaxTxnTimestamp)];
    EncodeFixed64(ts_buf, read_timestamp_);
    Slice ts(ts_buf, sizeof(ts_buf));
    read_opts_copy.timestamp = &ts;
    return TransactionBaseImpl::GetForUpdate(read_opts_copy, column_family, key,
                                             value, exclusive, do_validate);
  }
  assert(read_options.timestamp);
  const char* const ts_buf = read_options.timestamp->data();
  assert(read_options.timestamp->size() == sizeof(kMaxTxnTimestamp));
  TxnTimestamp ts = DecodeFixed64(ts_buf);
  if (ts != read_timestamp_) {
    return Status::InvalidArgument("Must read from the same read_timestamp");
  }
  return TransactionBaseImpl::GetForUpdate(read_options, column_family, key,
                                           value, exclusive, do_validate);
}

Status WriteCommittedTxn::GetEntityForUpdate(const ReadOptions& read_options,
                                             ColumnFamilyHandle* column_family,
                                             const Slice& key,
                                             PinnableWideColumns* columns,
                                             bool exclusive, bool do_validate) {
  if (!column_family) {
    return Status::InvalidArgument(
        "Cannot call GetEntityForUpdate without a column family handle");
  }

  const Comparator* const ucmp = column_family->GetComparator();
  assert(ucmp);
  const size_t ts_sz = ucmp->timestamp_size();

  if (ts_sz == 0) {
    return TransactionBaseImpl::GetEntityForUpdate(
        read_options, column_family, key, columns, exclusive, do_validate);
  }

  assert(ts_sz > 0);
  Status s = SanityCheckReadTimestamp(do_validate);
  if (!s.ok()) {
    return s;
  }

  std::string ts_buf;
  PutFixed64(&ts_buf, read_timestamp_);
  Slice ts(ts_buf);

  if (!read_options.timestamp) {
    ReadOptions read_options_copy = read_options;
    read_options_copy.timestamp = &ts;

    return TransactionBaseImpl::GetEntityForUpdate(
        read_options_copy, column_family, key, columns, exclusive, do_validate);
  }

  assert(read_options.timestamp);
  if (*read_options.timestamp != ts) {
    return Status::InvalidArgument("Must read from the same read timestamp");
  }

  return TransactionBaseImpl::GetEntityForUpdate(
      read_options, column_family, key, columns, exclusive, do_validate);
}

Status WriteCommittedTxn::SanityCheckReadTimestamp(bool do_validate) {
  bool enable_udt_validation =
      txn_db_impl_->GetTxnDBOptions().enable_udt_validation;
  if (!enable_udt_validation) {
    if (kMaxTxnTimestamp != read_timestamp_) {
      return Status::InvalidArgument(
          "read_timestamp is set but timestamp validation is disabled for the "
          "DB");
    }
  } else {
    if (!do_validate) {
      if (kMaxTxnTimestamp != read_timestamp_) {
        return Status::InvalidArgument(
            "If do_validate is false then GetForUpdate with read_timestamp is "
            "not "
            "defined.");
      }
    } else {
      if (kMaxTxnTimestamp == read_timestamp_) {
        return Status::InvalidArgument(
            "read_timestamp must be set for validation");
      }
    }
  }
  return Status::OK();
}

Status WriteCommittedTxn::PutEntityImpl(ColumnFamilyHandle* column_family,
                                        const Slice& key,
                                        const WideColumns& columns,
                                        bool do_validate, bool assume_tracked) {
  return Operate(column_family, key, do_validate, assume_tracked,
                 [column_family, &key, &columns, this]() {
                   Status s = GetBatchForWrite()->PutEntity(column_family, key,
                                                            columns);
                   if (s.ok()) {
                     ++num_put_entities_;
                   }
                   return s;
                 });
}

Status WriteCommittedTxn::Put(ColumnFamilyHandle* column_family,
                              const Slice& key, const Slice& value,
                              const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  return Operate(column_family, key, do_validate, assume_tracked,
                 [column_family, &key, &value, this]() {
                   Status s =
                       GetBatchForWrite()->Put(column_family, key, value);
                   if (s.ok()) {
                     ++num_puts_;
                   }
                   return s;
                 });
}

Status WriteCommittedTxn::Put(ColumnFamilyHandle* column_family,
                              const SliceParts& key, const SliceParts& value,
                              const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  return Operate(column_family, key, do_validate, assume_tracked,
                 [column_family, &key, &value, this]() {
                   Status s =
                       GetBatchForWrite()->Put(column_family, key, value);
                   if (s.ok()) {
                     ++num_puts_;
                   }
                   return s;
                 });
}

Status WriteCommittedTxn::PutUntracked(ColumnFamilyHandle* column_family,
                                       const Slice& key, const Slice& value) {
  return Operate(
      column_family, key, /*do_validate=*/false,
      /*assume_tracked=*/false, [column_family, &key, &value, this]() {
        Status s = GetBatchForWrite()->Put(column_family, key, value);
        if (s.ok()) {
          ++num_puts_;
        }
        return s;
      });
}

Status WriteCommittedTxn::PutUntracked(ColumnFamilyHandle* column_family,
                                       const SliceParts& key,
                                       const SliceParts& value) {
  return Operate(
      column_family, key, /*do_validate=*/false,
      /*assume_tracked=*/false, [column_family, &key, &value, this]() {
        Status s = GetBatchForWrite()->Put(column_family, key, value);
        if (s.ok()) {
          ++num_puts_;
        }
        return s;
      });
}

Status WriteCommittedTxn::Delete(ColumnFamilyHandle* column_family,
                                 const Slice& key, const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  return Operate(column_family, key, do_validate, assume_tracked,
                 [column_family, &key, this]() {
                   Status s = GetBatchForWrite()->Delete(column_family, key);
                   if (s.ok()) {
                     ++num_deletes_;
                   }
                   return s;
                 });
}

Status WriteCommittedTxn::Delete(ColumnFamilyHandle* column_family,
                                 const SliceParts& key,
                                 const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  return Operate(column_family, key, do_validate, assume_tracked,
                 [column_family, &key, this]() {
                   Status s = GetBatchForWrite()->Delete(column_family, key);
                   if (s.ok()) {
                     ++num_deletes_;
                   }
                   return s;
                 });
}

Status WriteCommittedTxn::DeleteUntracked(ColumnFamilyHandle* column_family,
                                          const Slice& key) {
  return Operate(column_family, key, /*do_validate=*/false,
                 /*assume_tracked=*/false, [column_family, &key, this]() {
                   Status s = GetBatchForWrite()->Delete(column_family, key);
                   if (s.ok()) {
                     ++num_deletes_;
                   }
                   return s;
                 });
}

Status WriteCommittedTxn::DeleteUntracked(ColumnFamilyHandle* column_family,
                                          const SliceParts& key) {
  return Operate(column_family, key, /*do_validate=*/false,
                 /*assume_tracked=*/false, [column_family, &key, this]() {
                   Status s = GetBatchForWrite()->Delete(column_family, key);
                   if (s.ok()) {
                     ++num_deletes_;
                   }
                   return s;
                 });
}

Status WriteCommittedTxn::SingleDelete(ColumnFamilyHandle* column_family,
                                       const Slice& key,
                                       const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  return Operate(column_family, key, do_validate, assume_tracked,
                 [column_family, &key, this]() {
                   Status s =
                       GetBatchForWrite()->SingleDelete(column_family, key);
                   if (s.ok()) {
                     ++num_deletes_;
                   }
                   return s;
                 });
}

Status WriteCommittedTxn::SingleDelete(ColumnFamilyHandle* column_family,
                                       const SliceParts& key,
                                       const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  return Operate(column_family, key, do_validate, assume_tracked,
                 [column_family, &key, this]() {
                   Status s =
                       GetBatchForWrite()->SingleDelete(column_family, key);
                   if (s.ok()) {
                     ++num_deletes_;
                   }
                   return s;
                 });
}

Status WriteCommittedTxn::SingleDeleteUntracked(
    ColumnFamilyHandle* column_family, const Slice& key) {
  return Operate(column_family, key, /*do_validate=*/false,
                 /*assume_tracked=*/false, [column_family, &key, this]() {
                   Status s =
                       GetBatchForWrite()->SingleDelete(column_family, key);
                   if (s.ok()) {
                     ++num_deletes_;
                   }
                   return s;
                 });
}

Status WriteCommittedTxn::Merge(ColumnFamilyHandle* column_family,
                                const Slice& key, const Slice& value,
                                const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  return Operate(column_family, key, do_validate, assume_tracked,
                 [column_family, &key, &value, this]() {
                   Status s =
                       GetBatchForWrite()->Merge(column_family, key, value);
                   if (s.ok()) {
                     ++num_merges_;
                   }
                   return s;
                 });
}

template <typename TKey, typename TOperation>
Status WriteCommittedTxn::Operate(ColumnFamilyHandle* column_family,
                                  const TKey& key, const bool do_validate,
                                  const bool assume_tracked,
                                  TOperation&& operation) {
  Status s;
  if constexpr (std::is_same_v<Slice, TKey>) {
    s = TryLock(column_family, key, /*read_only=*/false, /*exclusive=*/true,
                do_validate, assume_tracked);
  } else if constexpr (std::is_same_v<SliceParts, TKey>) {
    std::string key_buf;
    Slice contiguous_key(key, &key_buf);
    s = TryLock(column_family, contiguous_key, /*read_only=*/false,
                /*exclusive=*/true, do_validate, assume_tracked);
  }
  if (!s.ok()) {
    return s;
  }
  column_family =
      column_family ? column_family : db_impl_->DefaultColumnFamily();
  assert(column_family);
  const Comparator* const ucmp = column_family->GetComparator();
  assert(ucmp);
  size_t ts_sz = ucmp->timestamp_size();
  if (ts_sz > 0) {
    assert(ts_sz == sizeof(TxnTimestamp));
    if (!IndexingEnabled()) {
      cfs_with_ts_tracked_when_indexing_disabled_.insert(
          column_family->GetID());
    }
  }
  return operation();
}

Status WriteCommittedTxn::SetReadTimestampForValidation(TxnTimestamp ts) {
  if (read_timestamp_ < kMaxTxnTimestamp && ts < read_timestamp_) {
    return Status::InvalidArgument(
        "Cannot decrease read timestamp for validation");
  }
  read_timestamp_ = ts;
  return Status::OK();
}

Status WriteCommittedTxn::SetCommitTimestamp(TxnTimestamp ts) {
  if (txn_db_impl_->GetTxnDBOptions().enable_udt_validation &&
      read_timestamp_ < kMaxTxnTimestamp && ts <= read_timestamp_) {
    return Status::InvalidArgument(
        "Cannot commit at timestamp smaller than or equal to read timestamp");
  }
  commit_timestamp_ = ts;
  return Status::OK();
}

Status PessimisticTransaction::CommitBatch(WriteBatch* batch) {
  if (batch && WriteBatchInternal::HasKeyWithTimestamp(*batch)) {
    // CommitBatch() needs to lock the keys in the batch.
    // However, the application also needs to specify the timestamp for the
    // keys in batch before calling this API.
    // This means timestamp order may violate the order of locking, thus
    // violate the sequence number order for the same user key.
    // Therefore, we disallow this operation for now.
    return Status::NotSupported(
        "Batch to commit includes timestamp assigned before locking");
  }

  std::unique_ptr<LockTracker> keys_to_unlock(lock_tracker_factory_.Create());
  Status s = LockBatch(batch, keys_to_unlock.get());

  if (!s.ok()) {
    return s;
  }

  bool can_commit = false;

  if (IsExpired()) {
    s = Status::Expired();
  } else if (expiration_time_ > 0) {
    TransactionState expected = STARTED;
    can_commit = std::atomic_compare_exchange_strong(&txn_state_, &expected,
                                                     AWAITING_COMMIT);
  } else if (txn_state_ == STARTED) {
    // lock stealing is not a concern
    can_commit = true;
  }

  if (can_commit) {
    txn_state_.store(AWAITING_COMMIT);
    s = CommitBatchInternal(batch);
    if (s.ok()) {
      txn_state_.store(COMMITTED);
    }
  } else if (txn_state_ == LOCKS_STOLEN) {
    s = Status::Expired();
  } else {
    s = Status::InvalidArgument("Transaction is not in state for commit.");
  }

  txn_db_impl_->UnLock(this, *keys_to_unlock);

  return s;
}

Status PessimisticTransaction::Prepare() {
  if (name_.empty()) {
    return Status::InvalidArgument(
        "Cannot prepare a transaction that has not been named.");
  }

  if (IsExpired()) {
    return Status::Expired();
  }

  Status s;
  bool can_prepare = false;

  if (expiration_time_ > 0) {
    // must concern ourselves with expiraton and/or lock stealing
    // need to compare/exchange bc locks could be stolen under us here
    TransactionState expected = STARTED;
    can_prepare = std::atomic_compare_exchange_strong(&txn_state_, &expected,
                                                      AWAITING_PREPARE);
  } else if (txn_state_ == STARTED) {
    // expiration and lock stealing is not possible
    txn_state_.store(AWAITING_PREPARE);
    can_prepare = true;
  }

  if (can_prepare) {
    // transaction can't expire after preparation
    expiration_time_ = 0;
    assert(log_number_ == 0 ||
           txn_db_impl_->GetTxnDBOptions().write_policy == WRITE_UNPREPARED);

    s = PrepareInternal();
    if (s.ok()) {
      txn_state_.store(PREPARED);
    }
  } else if (txn_state_ == LOCKS_STOLEN) {
    s = Status::Expired();
  } else if (txn_state_ == PREPARED) {
    s = Status::InvalidArgument("Transaction has already been prepared.");
  } else if (txn_state_ == COMMITTED) {
    s = Status::InvalidArgument("Transaction has already been committed.");
  } else if (txn_state_ == ROLLEDBACK) {
    s = Status::InvalidArgument("Transaction has already been rolledback.");
  } else {
    s = Status::InvalidArgument("Transaction is not in state for commit.");
  }

  return s;
}

Status WriteCommittedTxn::PrepareInternal() {
  WriteOptions write_options = write_options_;
  write_options.disableWAL = false;
  auto s = WriteBatchInternal::MarkEndPrepare(GetWriteBatch()->GetWriteBatch(),
                                              name_);
  assert(s.ok());
  class MarkLogCallback : public PreReleaseCallback {
   public:
    MarkLogCallback(DBImpl* db, bool two_write_queues)
        : db_(db), two_write_queues_(two_write_queues) {
      (void)two_write_queues_;  // to silence unused private field warning
    }
    Status Callback(SequenceNumber, bool is_mem_disabled, uint64_t log_number,
                    size_t /*index*/, size_t /*total*/) override {
#ifdef NDEBUG
      (void)is_mem_disabled;
#endif
      assert(log_number != 0);
      assert(!two_write_queues_ || is_mem_disabled);  // implies the 2nd queue
      db_->logs_with_prep_tracker()->MarkLogAsContainingPrepSection(log_number);
      return Status::OK();
    }

   private:
    DBImpl* db_;
    bool two_write_queues_;
  } mark_log_callback(db_impl_,
                      db_impl_->immutable_db_options().two_write_queues);

  WriteCallback* const kNoWriteCallback = nullptr;
  const uint64_t kRefNoLog = 0;
  const bool kDisableMemtable = true;
  SequenceNumber* const KIgnoreSeqUsed = nullptr;
  const size_t kNoBatchCount = 0;
  s = db_impl_->WriteImpl(write_options, GetWriteBatch()->GetWriteBatch(),
                          kNoWriteCallback, /*user_write_cb=*/nullptr,
                          &log_number_, kRefNoLog, kDisableMemtable,
                          KIgnoreSeqUsed, kNoBatchCount, &mark_log_callback);
  return s;
}

Status PessimisticTransaction::Commit() {
  bool commit_without_prepare = false;
  bool commit_prepared = false;

  if (IsExpired()) {
    return Status::Expired();
  }

  if (expiration_time_ > 0) {
    // we must atomicaly compare and exchange the state here because at
    // this state in the transaction it is possible for another thread
    // to change our state out from under us in the even that we expire and have
    // our locks stolen. In this case the only valid state is STARTED because
    // a state of PREPARED would have a cleared expiration_time_.
    TransactionState expected = STARTED;
    commit_without_prepare = std::atomic_compare_exchange_strong(
        &txn_state_, &expected, AWAITING_COMMIT);
    TEST_SYNC_POINT("TransactionTest::ExpirableTransactionDataRace:1");
  } else if (txn_state_ == PREPARED) {
    // expiration and lock stealing is not a concern
    commit_prepared = true;
  } else if (txn_state_ == STARTED) {
    // expiration and lock stealing is not a concern
    if (skip_prepare_) {
      commit_without_prepare = true;
    } else {
      return Status::TxnNotPrepared();
    }
  }

  Status s;
  if (commit_without_prepare) {
    assert(!commit_prepared);
    if (WriteBatchInternal::Count(GetCommitTimeWriteBatch()) > 0) {
      s = Status::InvalidArgument(
          "Commit-time batch contains values that will not be committed.");
    } else {
      txn_state_.store(AWAITING_COMMIT);
      if (log_number_ > 0) {
        dbimpl_->logs_with_prep_tracker()->MarkLogAsHavingPrepSectionFlushed(
            log_number_);
      }
      s = CommitWithoutPrepareInternal();
      if (!name_.empty()) {
        txn_db_impl_->UnregisterTransaction(this);
      }
      Clear();
      if (s.ok()) {
        txn_state_.store(COMMITTED);
      }
    }
  } else if (commit_prepared) {
    txn_state_.store(AWAITING_COMMIT);

    s = CommitInternal();

    if (!s.ok()) {
      ROCKS_LOG_WARN(db_impl_->immutable_db_options().info_log,
                     "Commit write failed");
      // Keep the transaction rollbackable after the commit marker write fails.
      txn_state_.store(PREPARED);
      return s;
    }

    // FindObsoleteFiles must now look to the memtables
    // to determine what prep logs must be kept around,
    // not the prep section heap.
    assert(log_number_ > 0);
    dbimpl_->logs_with_prep_tracker()->MarkLogAsHavingPrepSectionFlushed(
        log_number_);
    txn_db_impl_->UnregisterTransaction(this);

    Clear();
    txn_state_.store(COMMITTED);
  } else if (txn_state_ == LOCKS_STOLEN) {
    s = Status::Expired();
  } else if (txn_state_ == COMMITTED) {
    s = Status::InvalidArgument("Transaction has already been committed.");
  } else if (txn_state_ == ROLLEDBACK) {
    s = Status::InvalidArgument("Transaction has already been rolledback.");
  } else {
    s = Status::InvalidArgument("Transaction is not in state for commit.");
  }

  return s;
}

Status WriteCommittedTxn::CommitWithoutPrepareInternal() {
  WriteBatchWithIndex* wbwi = GetWriteBatch();
  assert(wbwi);
  WriteBatch* wb = wbwi->GetWriteBatch();
  assert(wb);

  const bool needs_ts = WriteBatchInternal::HasKeyWithTimestamp(*wb);
  if (needs_ts && commit_timestamp_ == kMaxTxnTimestamp) {
    return Status::InvalidArgument("Must assign a commit timestamp");
  }

  if (needs_ts) {
    assert(commit_timestamp_ != kMaxTxnTimestamp);
    char commit_ts_buf[sizeof(kMaxTxnTimestamp)];
    EncodeFixed64(commit_ts_buf, commit_timestamp_);
    Slice commit_ts(commit_ts_buf, sizeof(commit_ts_buf));

    Status s = wb->UpdateTimestamps(
        commit_ts, [wb, wbwi, this](uint32_t cf) -> size_t {
          // First search through timestamp info kept inside the WriteBatch
          // in case some writes bypassed the Transaction's write APIs.
          auto cf_id_to_ts_sz = wb->GetColumnFamilyToTimestampSize();
          auto iter = cf_id_to_ts_sz.find(cf);
          if (iter != cf_id_to_ts_sz.end()) {
            size_t ts_sz = iter->second;
            return ts_sz;
          }
          auto cf_iter = cfs_with_ts_tracked_when_indexing_disabled_.find(cf);
          if (cf_iter != cfs_with_ts_tracked_when_indexing_disabled_.end()) {
            return sizeof(kMaxTxnTimestamp);
          }
          const Comparator* ucmp =
              WriteBatchWithIndexInternal::GetUserComparator(*wbwi, cf);
          return ucmp ? ucmp->timestamp_size()
                      : std::numeric_limits<size_t>::max();
        });
    if (!s.ok()) {
      return s;
    }
  }

  uint64_t seq_used = kMaxSequenceNumber;
  SnapshotCreationCallback snapshot_creation_cb(db_impl_, commit_timestamp_,
                                                snapshot_notifier_, snapshot_);
  PostMemTableCallback* post_mem_cb = nullptr;
  if (snapshot_needed_) {
    if (commit_timestamp_ == kMaxTxnTimestamp) {
      return Status::InvalidArgument("Must set transaction commit timestamp");
    } else {
      post_mem_cb = &snapshot_creation_cb;
    }
  }
  auto s = db_impl_->WriteImpl(
      write_options_, wb,
      /*callback*/ nullptr, /*user_write_cb=*/nullptr, /*wal_used*/ nullptr,
      /*log_ref*/ 0, /*disable_memtable*/ false, &seq_used, /*batch_cnt=*/0,
      /*pre_release_callback=*/nullptr, post_mem_cb);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  if (s.ok()) {
    SetId(seq_used);
  }
  return s;
}

Status WriteCommittedTxn::CommitBatchInternal(WriteBatch* batch, size_t) {
  uint64_t seq_used = kMaxSequenceNumber;
  auto s = db_impl_->WriteImpl(write_options_, batch, /*callback*/ nullptr,
                               /*user_write_cb=*/nullptr,
                               /*wal_used*/ nullptr, /*log_ref*/ 0,
                               /*disable_memtable*/ false, &seq_used);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  if (s.ok()) {
    SetId(seq_used);
  }
  return s;
}

Status WriteCommittedTxn::CommitInternal() {
  WriteBatchWithIndex* wbwi = GetWriteBatch();
  assert(wbwi);
  WriteBatch* wb = wbwi->GetWriteBatch();
  assert(wb);

  const bool needs_ts = WriteBatchInternal::HasKeyWithTimestamp(*wb);
  if (needs_ts && commit_timestamp_ == kMaxTxnTimestamp) {
    return Status::InvalidArgument("Must assign a commit timestamp");
  }
  // We take the commit-time batch and append the Commit marker.
  // The Memtable will ignore the Commit marker in non-recovery mode
  WriteBatch* working_batch = GetCommitTimeWriteBatch();

  Status s;
  if (!needs_ts) {
    s = WriteBatchInternal::MarkCommit(working_batch, name_);
  } else {
    assert(!commit_bypass_memtable_threshold_);
    assert(!commit_bypass_memtable_byte_threshold_);
    assert(commit_timestamp_ != kMaxTxnTimestamp);
    char commit_ts_buf[sizeof(kMaxTxnTimestamp)];
    EncodeFixed64(commit_ts_buf, commit_timestamp_);
    Slice commit_ts(commit_ts_buf, sizeof(commit_ts_buf));
    s = WriteBatchInternal::MarkCommitWithTimestamp(working_batch, name_,
                                                    commit_ts);
    if (s.ok()) {
      s = wb->UpdateTimestamps(
          commit_ts, [wb, wbwi, this](uint32_t cf) -> size_t {
            // Writes to a column family that had been dropped when this
            // transaction was rebuilt from the WAL are skipped: the memtable
            // insert ignores or rejects them, and the comparator lookup below
            // would fall back to the default column family's comparator.
            if (dropped_cfs_in_rebuilt_batch_.find(cf) !=
                dropped_cfs_in_rebuilt_batch_.end()) {
              return 0;
            }
            // first search through timestamp info kept inside the WriteBatch
            // in case some writes bypassed the Transaction's write APIs.
            auto cf_id_to_ts_sz = wb->GetColumnFamilyToTimestampSize();
            auto iter = cf_id_to_ts_sz.find(cf);
            if (iter != cf_id_to_ts_sz.end()) {
              return iter->second;
            }
            if (cfs_with_ts_tracked_when_indexing_disabled_.find(cf) !=
                cfs_with_ts_tracked_when_indexing_disabled_.end()) {
              return sizeof(kMaxTxnTimestamp);
            }
            const Comparator* ucmp =
                WriteBatchWithIndexInternal::GetUserComparator(*wbwi, cf);
            return ucmp ? ucmp->timestamp_size()
                        : std::numeric_limits<size_t>::max();
          });
    }
  }

  if (!s.ok()) {
    return s;
  }

  // any operations appended to this working_batch will be ignored from WAL
  working_batch->MarkWalTerminationPoint();

  uint32_t wb_count = wb->Count();
  RecordInHistogram(db_impl_->immutable_db_options_.stats,
                    NUM_OP_PER_TRANSACTION, wb_count);
  bool bypass_memtable = false;
  if (!needs_ts) {
    if (commit_bypass_memtable_threshold_ &&
        wb_count >= commit_bypass_memtable_threshold_) {
      if (wbwi->GetWBWIOpCount() != wb_count) {
        ROCKS_LOG_WARN(
            db_impl_->immutable_db_options().info_log,
            "Transaction %s qualifies for commit optimization due to update "
            "count. However, it will commit normally due to wbwi and wb record "
            "count mismatch. Some updates were added directly to the "
            "transaction's underlying write batch.",
            GetName().c_str());
      } else {
        bypass_memtable = true;
      }
    } else if (commit_bypass_memtable_byte_threshold_ &&
               wb->GetDataSize() >= commit_bypass_memtable_byte_threshold_) {
      if (wbwi->GetWBWIOpCount() != wb_count) {
        ROCKS_LOG_WARN(
            db_impl_->immutable_db_options().info_log,
            "Transaction %s qualifies for commit optimization due to write "
            "batch size. However, it will commit normally due to wbwi and wb "
            "record count mismatch. Some updates were added directly to the "
            "transaction's underlying write batch.",
            GetName().c_str());
      } else {
        bypass_memtable = true;
      }
    }
  }
  // Blob direct write must go through the normal WriteBatch path in
  // DBImpl::WriteImpl() so large values can be transformed into BlobIndex
  // entries. The bypass-memtable optimization commits through WBWI ingestion,
  // which skips that transformation and does not support kTypeBlobIndex.
  if (bypass_memtable && db_impl_->HasAnyBlobDirectWriteColumnFamily()) {
    bypass_memtable = false;
  }
  if (!bypass_memtable) {
    // insert prepared batch into Memtable only skipping WAL.
    // Memtable will ignore BeginPrepare/EndPrepare markers
    // in non recovery mode and simply insert the values
    s = WriteBatchInternal::Append(working_batch, wb);
    assert(s.ok());
  }

  uint64_t seq_used = kMaxSequenceNumber;
  SnapshotCreationCallback snapshot_creation_cb(db_impl_, commit_timestamp_,
                                                snapshot_notifier_, snapshot_);
  PostMemTableCallback* post_mem_cb = nullptr;
  if (snapshot_needed_) {
    if (commit_timestamp_ == kMaxTxnTimestamp) {
      s = Status::InvalidArgument("Must set transaction commit timestamp");
      return s;
    } else {
      post_mem_cb = &snapshot_creation_cb;
    }
  }
  assert(log_number_ > 0);
  TEST_SYNC_POINT_CALLBACK("WriteCommittedTxn::CommitInternal:bypass_memtable",
                           static_cast<void*>(&bypass_memtable));
  if (bypass_memtable) {
    // Used for differentiating commiting WBWI vs directly ingesting WBWI
    // see (IngestWriteBatchWithIndex())
    assert(working_batch->HasCommit());
    s = db_impl_->WriteImpl(
        write_options_, working_batch, /*callback*/ nullptr,
        /*user_write_cb=*/nullptr,
        /*wal_used*/ nullptr, /*log_ref*/ log_number_,
        /*disable_memtable*/ false, &seq_used,
        /*batch_cnt=*/0, /*pre_release_callback=*/nullptr, post_mem_cb,
        /*wbwi=*/
        std::make_shared<WriteBatchWithIndex>(std::move(write_batch_)));
    // Reset write_batch_ since it's accessed in transaction clean up and
    // might be used for transaction reuse.
    write_batch_ = WriteBatchWithIndex(cmp_, 0, true, 0,
                                       write_options_.protection_bytes_per_key);
  } else {
    s = db_impl_->WriteImpl(write_options_, working_batch, /*callback*/ nullptr,
                            /*user_write_cb=*/nullptr,
                            /*wal_used*/ nullptr, /*log_ref*/ log_number_,
                            /*disable_memtable*/ false, &seq_used,
                            /*batch_cnt=*/0, /*pre_release_callback=*/nullptr,
                            post_mem_cb);
  }
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  if (s.ok()) {
    SetId(seq_used);
  }
  return s;
}

Status PessimisticTransaction::Rollback() {
  Status s;
  if (txn_state_ == PREPARED) {
    txn_state_.store(AWAITING_ROLLBACK);

    s = RollbackInternal();

    if (s.ok()) {
      // we do not need to keep our prepared section around
      assert(log_number_ > 0);
      dbimpl_->logs_with_prep_tracker()->MarkLogAsHavingPrepSectionFlushed(
          log_number_);
      Clear();
      txn_state_.store(ROLLEDBACK);
    } else {
      // Rollback writes can fail under retryable IO errors. Preserve the state
      // so callers can retry after error recovery.
      txn_state_.store(PREPARED);
    }
  } else if (txn_state_ == STARTED) {
    if (log_number_ > 0) {
      assert(txn_db_impl_->GetTxnDBOptions().write_policy == WRITE_UNPREPARED);
      assert(GetId() > 0);
      s = RollbackInternal();

      if (s.ok()) {
        dbimpl_->logs_with_prep_tracker()->MarkLogAsHavingPrepSectionFlushed(
            log_number_);
      }
    }
    // prepare couldn't have taken place
    Clear();
  } else if (txn_state_ == COMMITTED) {
    s = Status::InvalidArgument("This transaction has already been committed.");
  } else {
    s = Status::InvalidArgument(
        "Two phase transaction is not in state for rollback.");
  }

  return s;
}

Status WriteCommittedTxn::RollbackInternal() {
  WriteBatch rollback_marker;
  auto s = WriteBatchInternal::MarkRollback(&rollback_marker, name_);
  assert(s.ok());
  s = db_impl_->WriteImpl(write_options_, &rollback_marker);
  return s;
}

Status PessimisticTransaction::RollbackToSavePoint() {
  if (txn_state_ != STARTED) {
    return Status::InvalidArgument("Transaction is beyond state for rollback.");
  }

  if (save_points_ != nullptr && !save_points_->empty()) {
    // Unlock any keys locked since last transaction
    auto& save_point_tracker = *save_points_->top().new_locks_;
    std::unique_ptr<LockTracker> t(
        tracked_locks_->GetTrackedLocksSinceSavePoint(save_point_tracker));
    if (t) {
      txn_db_impl_->UnLock(this, *t);
    }
  }

  return TransactionBaseImpl::RollbackToSavePoint();
}

// Lock all keys in this batch.
// On success, caller should unlock keys_to_unlock
Status PessimisticTransaction::LockBatch(WriteBatch* batch,
                                         LockTracker* keys_to_unlock) {
  if (!batch) {
    return Status::InvalidArgument("batch is nullptr");
  }

  class Handler : public WriteBatch::Handler {
   public:
    // Sorted map of column_family_id to sorted set of keys.
    // Since LockBatch() always locks keys in sorted order, it cannot deadlock
    // with itself.  We're not using a comparator here since it doesn't matter
    // what the sorting is as long as it's consistent.
    std::map<uint32_t, std::set<std::string>> keys_;

    Handler() = default;

    void RecordKey(uint32_t column_family_id, const Slice& key) {
      auto& cfh_keys = keys_[column_family_id];
      cfh_keys.insert(key.ToString());
    }

    Status PutCF(uint32_t column_family_id, const Slice& key,
                 const Slice& /* unused */) override {
      RecordKey(column_family_id, key);
      return Status::OK();
    }
    Status PutEntityCF(uint32_t column_family_id, const Slice& key,
                       const Slice& /* unused */) override {
      RecordKey(column_family_id, key);
      return Status::OK();
    }
    Status MergeCF(uint32_t column_family_id, const Slice& key,
                   const Slice& /* unused */) override {
      RecordKey(column_family_id, key);
      return Status::OK();
    }
    Status DeleteCF(uint32_t column_family_id, const Slice& key) override {
      RecordKey(column_family_id, key);
      return Status::OK();
    }
  };

  // Iterating on this handler will add all keys in this batch into keys
  Handler handler;
  Status s = batch->Iterate(&handler);
  if (!s.ok()) {
    return s;
  }

  // Attempt to lock all keys
  for (const auto& cf_iter : handler.keys_) {
    uint32_t cfh_id = cf_iter.first;
    auto& cfh_keys = cf_iter.second;

    for (const auto& key_iter : cfh_keys) {
      const std::string& key = key_iter;

      s = txn_db_impl_->TryLock(this, cfh_id, key, true /* exclusive */);
      if (!s.ok()) {
        break;
      }
      PointLockRequest r;
      r.column_family_id = cfh_id;
      r.key = key;
      r.seq = kMaxSequenceNumber;
      r.read_only = false;
      r.exclusive = true;
      keys_to_unlock->Track(r);
    }

    if (!s.ok()) {
      break;
    }
  }

  if (!s.ok()) {
    txn_db_impl_->UnLock(this, *keys_to_unlock);
  }

  return s;
}

// Attempt to lock this key.
// Returns OK if the key has been successfully locked.  Non-ok, otherwise.
// If check_shapshot is true and this transaction has a snapshot set,
// this key will only be locked if there have been no writes to this key since
// the snapshot time.
Status PessimisticTransaction::TryLock(ColumnFamilyHandle* column_family,
                                       const Slice& key, bool read_only,
                                       bool exclusive, const bool do_validate,
                                       const bool assume_tracked) {
  assert(!assume_tracked || !do_validate);
  Status s;
  if (UNLIKELY(skip_concurrency_control_)) {
    return s;
  }
  uint32_t cfh_id = GetColumnFamilyID(column_family);
  std::string key_str = key.ToString();

  PointLockStatus status;
  bool lock_upgrade;
  bool previously_locked;
  if (tracked_locks_->IsPointLockSupported()) {
    status = tracked_locks_->GetPointLockStatus(cfh_id, key_str);
    previously_locked = status.locked;
    lock_upgrade = previously_locked && exclusive && !status.exclusive;
  } else {
    // If the record is tracked, we can assume it was locked, too.
    previously_locked = assume_tracked;
    status.locked = false;
    lock_upgrade = false;
  }

  // Lock this key if this transactions hasn't already locked it or we require
  // an upgrade.
  if (!previously_locked || lock_upgrade) {
    s = txn_db_impl_->TryLock(this, cfh_id, key_str, exclusive);
  }

  const ColumnFamilyHandle* const cfh =
      column_family ? column_family : db_impl_->DefaultColumnFamily();
  assert(cfh);
  const Comparator* const ucmp = cfh->GetComparator();
  assert(ucmp);
  size_t ts_sz = ucmp->timestamp_size();

  SetSnapshotIfNeeded();

  // Even though we do not care about doing conflict checking for this write,
  // we still need to take a lock to make sure we do not cause a conflict with
  // some other write.  However, we do not need to check if there have been
  // any writes since this transaction's snapshot.
  // TODO(agiardullo): could optimize by supporting shared txn locks in the
  // future.
  SequenceNumber tracked_at_seq =
      status.locked ? status.seq : kMaxSequenceNumber;
  if (!do_validate || (snapshot_ == nullptr &&
                       (0 == ts_sz || kMaxTxnTimestamp == read_timestamp_))) {
    if (assume_tracked && !previously_locked &&
        tracked_locks_->IsPointLockSupported()) {
      s = Status::InvalidArgument(
          "assume_tracked is set but it is not tracked yet");
    }
    // Need to remember the earliest sequence number that we know that this
    // key has not been modified after.  This is useful if this same
    // transaction later tries to lock this key again.
    if (tracked_at_seq == kMaxSequenceNumber) {
      // Since we haven't checked a snapshot, we only know this key has not
      // been modified since after we locked it.
      // Note: when last_seq_same_as_publish_seq_==false this is less than the
      // latest allocated seq but it is ok since i) this is just a heuristic
      // used only as a hint to avoid actual check for conflicts, ii) this would
      // cause a false positive only if the snapthot is taken right after the
      // lock, which would be an unusual sequence.
      tracked_at_seq = db_->GetLatestSequenceNumber();
    }
  } else if (s.ok()) {
    // If a snapshot is set, we need to make sure the key hasn't been modified
    // since the snapshot.  This must be done after we locked the key.
    // If we already have validated an earilier snapshot it must has been
    // reflected in tracked_at_seq and ValidateSnapshot will return OK.
    s = ValidateSnapshot(column_family, key, &tracked_at_seq);

    if (!s.ok()) {
      // Failed to validate key
      // Unlock key we just locked
      if (lock_upgrade) {
        s = txn_db_impl_->TryLock(this, cfh_id, key_str, false /* exclusive */);
        assert(s.ok());
      } else if (!previously_locked) {
        txn_db_impl_->UnLock(this, cfh_id, key.ToString());
      }
    }
  }

  if (s.ok()) {
    // We must track all the locked keys so that we can unlock them later. If
    // the key is already locked, this func will update some stats on the
    // tracked key. It could also update the tracked_at_seq if it is lower
    // than the existing tracked key seq. These stats are necessary for
    // RollbackToSavePoint to determine whether a key can be safely removed
    // from tracked_keys_. Removal can only be done if a key was only locked
    // during the current savepoint.
    //
    // Recall that if assume_tracked is true, we assume that TrackKey has been
    // called previously since the last savepoint, with the same exclusive
    // setting, and at a lower sequence number, so skipping here should be
    // safe.
    if (!assume_tracked) {
      TrackKey(cfh_id, key_str, tracked_at_seq, read_only, exclusive);
    } else {
#ifndef NDEBUG
      if (tracked_locks_->IsPointLockSupported()) {
        PointLockStatus lock_status =
            tracked_locks_->GetPointLockStatus(cfh_id, key_str);
        assert(lock_status.locked);
        assert(lock_status.seq <= tracked_at_seq);
        assert(lock_status.exclusive == exclusive);
      }
#endif
    }
  }

  return s;
}

Status PessimisticTransaction::GetRangeLock(ColumnFamilyHandle* column_family,
                                            const Endpoint& start_endp,
                                            const Endpoint& end_endp) {
  ColumnFamilyHandle* cfh =
      column_family ? column_family : db_impl_->DefaultColumnFamily();
  uint32_t cfh_id = GetColumnFamilyID(cfh);

  Status s = txn_db_impl_->TryRangeLock(this, cfh_id, start_endp, end_endp);

  if (s.ok()) {
    RangeLockRequest req{cfh_id, start_endp, end_endp};
    tracked_locks_->Track(req);
  }
  return s;
}

// Return OK() if this key has not been modified more recently than the
// transaction snapshot_.
// tracked_at_seq is the global seq at which we either locked the key or already
// have done ValidateSnapshot.
Status PessimisticTransaction::ValidateSnapshot(
    ColumnFamilyHandle* column_family, const Slice& key,
    SequenceNumber* tracked_at_seq) {
  assert(snapshot_ || read_timestamp_ < kMaxTxnTimestamp);

  SequenceNumber snap_seq = 0;
  if (snapshot_) {
    snap_seq = snapshot_->GetSequenceNumber();
    if (*tracked_at_seq <= snap_seq) {
      // If the key has been previous validated (or locked) at a sequence number
      // earlier than the current snapshot's sequence number, we already know it
      // has not been modified aftter snap_seq either.
      return Status::OK();
    }
  } else {
    snap_seq = db_impl_->GetLatestSequenceNumber();
  }

  // Otherwise we have either
  // 1: tracked_at_seq == kMaxSequenceNumber, i.e., first time tracking the key
  // 2: snap_seq < tracked_at_seq: last time we lock the key was via
  // do_validate=false which means we had skipped ValidateSnapshot. In both
  // cases we should do ValidateSnapshot now.

  *tracked_at_seq = snap_seq;

  ColumnFamilyHandle* cfh =
      column_family ? column_family : db_impl_->DefaultColumnFamily();

  assert(cfh);
  const Comparator* const ucmp = cfh->GetComparator();
  assert(ucmp);
  size_t ts_sz = ucmp->timestamp_size();
  std::string ts_buf;
  if (ts_sz > 0 && read_timestamp_ < kMaxTxnTimestamp) {
    assert(ts_sz == sizeof(read_timestamp_));
    PutFixed64(&ts_buf, read_timestamp_);
  }

  return TransactionUtil::CheckKeyForConflicts(
      db_impl_, cfh, key.ToString(), snap_seq, ts_sz == 0 ? nullptr : &ts_buf,
      false /* cache_only */,
      /* snap_checker */ nullptr,
      /* min_uncommitted */ kMaxSequenceNumber,
      txn_db_impl_->GetTxnDBOptions().enable_udt_validation);
}

bool PessimisticTransaction::TryStealingLocks() {
  assert(IsExpired());
  TransactionState expected = STARTED;
  return std::atomic_compare_exchange_strong(&txn_state_, &expected,
                                             LOCKS_STOLEN);
}

void PessimisticTransaction::UnlockGetForUpdate(
    ColumnFamilyHandle* column_family, const Slice& key) {
  txn_db_impl_->UnLock(this, GetColumnFamilyID(column_family), key.ToString());
}

Status PessimisticTransaction::SetName(const TransactionName& name) {
  Status s;
  if (txn_state_ == STARTED) {
    if (name_.length()) {
      s = Status::InvalidArgument("Transaction has already been named.");
    } else if (name.length() < 1 || name.length() > 512) {
      s = Status::InvalidArgument(
          "Transaction name length must be between 1 and 512 chars.");
    } else {
      name_ = name;
      s = txn_db_impl_->RegisterTransaction(this);
      if (!s.ok()) {
        name_.clear();
      }
    }
  } else {
    s = Status::InvalidArgument("Transaction is beyond state for naming.");
  }
  return s;
}

Status PessimisticTransaction::CollapseKey(const ReadOptions& options,
                                           const Slice& key,
                                           ColumnFamilyHandle* column_family) {
  auto* cfh = column_family ? column_family : db_impl_->DefaultColumnFamily();
  std::string value;
  const auto status = GetForUpdate(options, cfh, key, &value, true, true);
  if (!status.ok()) {
    return status;
  }
  return Put(column_family, key, value);
}

}  // namespace ROCKSDB_NAMESPACE
