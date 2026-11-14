//  Copyright (c) 2019-present.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "db/column_family.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/totransaction_db.h"
#include "util/string_util.h"
#include "util/logging.h"
#include "util/cast_util.h"
#include "utilities/transactions/totransaction_db_impl.h"
#include "utilities/transactions/totransaction_impl.h"


namespace rocksdb {

struct WriteOptions;

std::atomic<TransactionID> TOTransactionImpl::txn_id_counter_(1);

TransactionID TOTransactionImpl::GenTxnID() {
  return txn_id_counter_.fetch_add(1);
}

TOTransactionImpl::TOTransactionImpl(TOTransactionDB* txn_db,
              const WriteOptions& write_options,
              const TOTxnOptions& txn_option,
              const std::shared_ptr<ActiveTxnNode>& core)
    : txn_id_(0),
      db_(txn_db->GetRootDB()), 
      write_options_(write_options),
      txn_option_(txn_option),
      cmp_(GetColumnFamilyUserComparator(db_->DefaultColumnFamily())),
      write_batch_(cmp_, 0, true, 0),
      core_(core) {
      txn_db_impl_ = static_cast_with_check<TOTransactionDBImpl, TOTransactionDB>(txn_db);
      assert(txn_db_impl_);
      db_impl_ = static_cast_with_check<DBImpl, DB>(txn_db->GetRootDB());
      Initialize();
}

void TOTransactionImpl::Initialize() {
  txn_id_ = GenTxnID();

  txn_state_ = kStarted;
}

TOTransactionImpl::~TOTransactionImpl() {
  // Do rollback if this transaction is not committed or rolled back
  if (txn_state_ < kCommitted) {
    Rollback();
  }
}

Status TOTransactionImpl::SetReadTimeStamp(const RocksTimeStamp& timestamp,
                                           const uint32_t& round) {
  if (txn_state_ >= kCommitted) {
    return Status::NotSupported("this txn is committed or rollback");
  } 
  
  if (core_->read_ts_set_) {
    return Status::NotSupported("set read ts is supposed to be set only once");
  }
  
  ROCKS_LOG_DEBUG(txn_option_.log_, "TOTDB txn id(%llu) set read ts(%llu) force(%d)\n",
                                                                txn_id_, timestamp, round);

  Status s = txn_db_impl_->AddReadQueue(core_, timestamp, round);
  if (!s.ok()) {
    return s;
  }
  assert(core_->read_ts_set_);
  assert(core_->read_ts_ >= timestamp);
  return s;
}

Status TOTransactionImpl::SetCommitTimeStamp(const RocksTimeStamp& timestamp) {
  if (txn_state_ >= kCommitted) {
    return Status::NotSupported("this txn is committed or rollback");
  }

  if (timestamp == 0) {
    return Status::NotSupported("not allowed to set committs to 0");
  }

  if (core_->commit_ts_set_) {
    if (core_->commit_ts_ > timestamp) {
      return Status::NotSupported("commit ts need equal with the pre set");
    }
    if (core_->commit_ts_ == timestamp) {
      return Status::OK();
    }
  }

  assert((!core_->commit_ts_set_) || core_->commit_ts_ < timestamp);

  // publish commit_ts to global view
  auto s = txn_db_impl_->AddCommitQueue(core_, timestamp);
  if (!s.ok()) {
    return s;
  }
  assert(core_->commit_ts_set_ && (core_->first_commit_ts_ <= core_->commit_ts_));

  ROCKS_LOG_DEBUG(txn_option_.log_, "TOTDB txn id(%llu) set commit ts(%llu)\n", 
                                                                core_->txn_id_, timestamp);
  return Status::OK();
}

Status TOTransactionImpl::GetReadTimeStamp(RocksTimeStamp* timestamp) const {
  if ((!timestamp) || (!core_->read_ts_set_)) {
      return Status::InvalidArgument("need set read ts, and parameter should not be null");
  }

  *timestamp = core_->read_ts_;

  return Status::OK();
}

WriteBatchWithIndex* TOTransactionImpl::GetWriteBatch() {
  return &write_batch_;
}

Status TOTransactionImpl::Put(ColumnFamilyHandle* column_family, const Slice& key,
           const Slice& value) {
  if (txn_db_impl_->IsReadOnly()) {
    return Status::NotSupported("readonly db cannot accept put");
  }
  if (txn_state_ >= kCommitted) {
    return Status::NotSupported("this txn is already committed or rollback");
  } 
  
  Status s = CheckWriteConflict(column_family, key);
  
  if (s.ok()) {
    writtenKeys_.insert(key.ToString());
    GetWriteBatch()->Put(column_family, key, value);
    write_options_.asif_commit_timestamps.emplace_back(core_->commit_ts_);
  }
  return s;
}

Status TOTransactionImpl::Put(const Slice& key, const Slice& value) {
  return Put(db_->DefaultColumnFamily(), key, value);
}

Status TOTransactionImpl::Get(ReadOptions& options,
           ColumnFamilyHandle* column_family, const Slice& key,
           std::string* value) {
  if (txn_state_ >= kCommitted) {
    return Status::NotSupported("this txn is already committed or rollback");
  } 
  // Check the options, if read ts is set use read ts
  options.read_timestamp = core_->read_ts_;
  options.snapshot = txn_option_.txn_snapshot;
    
  return write_batch_.GetFromBatchAndDB(db_, options, column_family, key,
                                        value);
}

Status TOTransactionImpl::Get(ReadOptions& options, const Slice& key,
           std::string* value) {
  return Get(options, db_->DefaultColumnFamily(), key, value);
}

Status TOTransactionImpl::Delete(ColumnFamilyHandle* column_family, const Slice& key) {
  if (txn_db_impl_->IsReadOnly()) {
    return Status::NotSupported("readonly db cannot accept del");
  }
  if (txn_state_ >= kCommitted) {
    return Status::NotSupported("this txn is already committed or rollback");
  } 
  Status s = CheckWriteConflict(column_family, key);

  if (s.ok()) {
    writtenKeys_.insert(key.ToString());
    GetWriteBatch()->Delete(column_family, key);
    write_options_.asif_commit_timestamps.emplace_back(core_->commit_ts_);
  }
  return s;
}

Status TOTransactionImpl::Delete(const Slice& key) {
  return Delete(db_->DefaultColumnFamily(), key);
}

Iterator* TOTransactionImpl::GetIterator(ReadOptions& read_options) {
  return GetIterator(read_options, db_->DefaultColumnFamily());
}

Iterator* TOTransactionImpl::GetIterator(ReadOptions& read_options,
                      ColumnFamilyHandle* column_family) {
  if (txn_state_ >= kCommitted) {
    return nullptr;
  } 
  
  read_options.read_timestamp = core_->read_ts_;

  read_options.snapshot = txn_option_.txn_snapshot;
  
  Iterator* db_iter = db_->NewIterator(read_options, column_family);
  if (db_iter == nullptr) {
    return nullptr;
  }

  return write_batch_.NewIteratorWithBase(column_family, db_iter);
}

Status TOTransactionImpl::CheckWriteConflict(ColumnFamilyHandle* column_family, 
                                             const Slice& key) {
  return txn_db_impl_->CheckWriteConflict(column_family, key, GetID(), core_->read_ts_);
}

Status TOTransactionImpl::Commit() {
  if (txn_state_ >= kCommitted) {
    return Status::InvalidArgument("txn already committed or rollback.");
  }
  
  assert(write_options_.asif_commit_timestamps.size()
    == static_cast<size_t>(GetWriteBatch()->GetWriteBatch()->Count()));
  if (core_->commit_ts_set_) {
    for (size_t i = 0; i < write_options_.asif_commit_timestamps.size(); ++i) {
      if (write_options_.asif_commit_timestamps[i] == 0) {
        write_options_.asif_commit_timestamps[i] = core_->commit_ts_;
      }
    }
  }
  Status s;
  if (GetWriteBatch()->GetWriteBatch()->Count() != 0) {
    assert(!txn_db_impl_->IsReadOnly());
    // NOTE(wolfkdy): It's a simple modification for readonly transaction.
    // PutLogData will not increase Count. So, If in the future
    // PutLogData is added into TOTransactionDB, this shortcut should be redesigned.
    s = db_->Write(write_options_, GetWriteBatch()->GetWriteBatch());
  }
  if (s.ok()) {
    txn_state_.store(kCommitted);
    // Change active txn set,
    // Move uncommitted keys to committed keys,
    // Clean data when the committed txn is activeTxnSet's header
    // TODO(wolfkdy): in fact, here we must not fail
    s = txn_db_impl_->CommitTransaction(core_, writtenKeys_);
  } else {
    s = Status::InvalidArgument("Transaction is fail for commit.");
  }
 
  ROCKS_LOG_DEBUG(txn_option_.log_, "TOTDB txn id(%llu) committed \n", txn_id_);
  return s;
}

Status TOTransactionImpl::Rollback() {
  if (txn_state_ >= kCommitted) {
    return Status::InvalidArgument("txn is already committed or rollback.");
  }
  
  GetWriteBatch()->Clear();
  
  txn_state_.store(kRollback);

  // Change active txn set,
  // Clean uncommitted keys
  Status s = txn_db_impl_->RollbackTransaction(core_, writtenKeys_);

  ROCKS_LOG_DEBUG(txn_option_.log_, "TOTDB txn id(%llu) rollback \n", txn_id_);
  return s;
}

Status TOTransactionImpl::SetName(const TransactionName& name) {
  name_ = name;
  return Status::OK();
}

}

#endif
