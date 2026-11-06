//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/transactions/totransaction_db_impl.h"
#include "util/string_util.h"
#include "util/logging.h"

namespace rocksdb {

Status TOTransactionDB::Open(const Options& options,
                     const TOTransactionDBOptions& txn_db_options,
                     const std::string& dbname, TOTransactionDB** dbptr) {
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
  ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  Status s = Open(db_options, txn_db_options, dbname, column_families, &handles, dbptr);
  if (s.ok()) {
    assert(handles.size() == 1);
    // I can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
  }

  return s;
}


Status TOTransactionDB::Open(const DBOptions& db_options,
                     const TOTransactionDBOptions& txn_db_options,
                     const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     TOTransactionDB** dbptr) {
  Status s;
  DB* db = nullptr;
	
  if(db_options.open_read_replica){
    ROCKS_LOG_INFO(db_options.info_log, "##### TOTDB open on readonly mode #####");
    s = DB::OpenForReadOnly(db_options, dbname, column_families, handles,
                            &db, false);
    if (s.ok()) {
      auto v = new TOTransactionDBImpl(db, txn_db_options, true);
      v->StartBackgroundCleanThread();
      *dbptr = v;
    }
  }else{
    ROCKS_LOG_INFO(db_options.info_log, "##### TOTDB open on normal mode #####");
    s = DB::Open(db_options, dbname, column_families, handles, &db);
    if (s.ok()) {
      auto v = new TOTransactionDBImpl(db, txn_db_options, false);
      v->StartBackgroundCleanThread();
      *dbptr = v;
    }
  }
  

  ROCKS_LOG_DEBUG(db_options.info_log, "##### TOTDB open success #####");

  return s;
} 

Status TOTransactionDBImpl::UnCommittedKeys::RemoveKeyInLock(const Slice& key,
                                                  const size_t& stripe_num,
                                                  std::atomic<int64_t>* mem_usage) {
  UnCommittedLockMapStripe* stripe = lock_map_stripes_.at(stripe_num);
  auto iter = stripe->uncommitted_keys_map_.find(key.ToString());
  assert(iter != stripe->uncommitted_keys_map_.end());
  auto ccbytes = mem_usage->fetch_sub(
    iter->first.size() + sizeof(iter->second),
    std::memory_order_relaxed);
  assert(ccbytes >= 0);
  (void)ccbytes;
  stripe->uncommitted_keys_map_.erase(iter);
  return Status::OK();
}

Status TOTransactionDBImpl::UnCommittedKeys::CheckKeyAndAddInLock(const Slice& key, 
                                             const TransactionID& txn_id,
                                             const size_t& stripe_num,
                                             const size_t& max_mem_usage, 
                                             std::atomic<int64_t>* mem_usage) {
  UnCommittedLockMapStripe* stripe = lock_map_stripes_.at(stripe_num);

  auto iter = stripe->uncommitted_keys_map_.find(key.ToString());
  if (iter != stripe->uncommitted_keys_map_.end()) {
    // Check whether the key is modified by the same txn
    if (iter->second != txn_id) {
      ROCKS_LOG_WARN(info_log_, "TOTDB WriteConflict another txn id(%llu) is modifying key(%s) \n",
        iter->second, key.ToString());
      return Status::Busy();
    } else {
      return Status::OK();
    }
  } 
  auto addedSize =
    mem_usage->load(std::memory_order_relaxed) + key.size() + sizeof(txn_id);
  if (addedSize > max_mem_usage) {
    ROCKS_LOG_WARN(info_log_, "TOTDB WriteConflict mem usage(%ll) is greater than limit(%llu) \n",
      addedSize, max_mem_usage);
    return Status::Busy();
  }
  mem_usage->fetch_add(key.size() + sizeof(txn_id), std::memory_order_relaxed);
  stripe->uncommitted_keys_map_.insert({key.ToString(), txn_id});

  return Status::OK(); 
}

size_t TOTransactionDBImpl::UnCommittedKeys::CountInLock() const {
  size_t res = 0;
  for (size_t i = 0; i < lock_map_stripes_.size(); ++i) {
    res += lock_map_stripes_[i]->uncommitted_keys_map_.size();
  }
  return res;
}

Status TOTransactionDBImpl::CommittedKeys::AddKeyInLock(const Slice& key,
              const TransactionID& commit_txn_id,
              const RocksTimeStamp& commit_ts,
              const size_t& stripe_num,
              std::atomic<int64_t>* mem_usage) {
  CommittedLockMapStripe* stripe = lock_map_stripes_.at(stripe_num); 
  if (stripe->committed_keys_map_.find(key.ToString()) == stripe->committed_keys_map_.end()) {
    mem_usage->fetch_add(key.size() + sizeof(commit_ts) + sizeof(commit_txn_id));
  }

  stripe->committed_keys_map_[key.ToString()] = {commit_txn_id, commit_ts};

  return Status::OK();
}
 
Status TOTransactionDBImpl::CommittedKeys::RemoveKeyInLock(const Slice& key, 
                                          const TransactionID& txn_id,
                                          const size_t& stripe_num,
                                          std::atomic<int64_t>* mem_usage) {
  CommittedLockMapStripe* stripe = lock_map_stripes_.at(stripe_num);

  auto iter = stripe->committed_keys_map_.find(key.ToString());
  assert(iter != stripe->committed_keys_map_.end());

  if (iter->second.first <= txn_id) {
    auto ccbytes = mem_usage->fetch_sub(
      key.size() + sizeof(iter->second.first) + sizeof(iter->second.second),
      std::memory_order_relaxed);
    assert(ccbytes >= 0);
    (void)ccbytes;
    stripe->committed_keys_map_.erase(iter);
  }

  return Status::OK();
}

Status TOTransactionDBImpl::CommittedKeys::CheckKeyInLock(const Slice& key, 
                    const TransactionID& txn_id,
                    const RocksTimeStamp& timestamp,
                    const size_t& stripe_num) {
  // padding to avoid false sharing
  CommittedLockMapStripe* stripe = lock_map_stripes_.at(stripe_num);

  auto iter = stripe->committed_keys_map_.find(key.ToString());
  if (iter != stripe->committed_keys_map_.end()) {
    // Check whether some txn commits the key after txn_id began
    const auto& back = iter->second;
    if (back.first > txn_id) {
      ROCKS_LOG_WARN(info_log_, "TOTDB WriteConflict a committed txn commit_id(%llu) greater than my txnid(%llu) \n", 
        back.first, txn_id);
      return Status::Busy();
    }
    // Find the latest committed txn for this key and its commit ts  
    if (back.second > timestamp) {
      ROCKS_LOG_WARN(info_log_, "TOTDB WriteConflict a committed txn commit_ts(%llu) greater than my read_ts(%llu) \n", 
        back.second, timestamp);
      return Status::Busy();
    }
  }
  return Status::OK();
}

size_t TOTransactionDBImpl::CommittedKeys::CountInLock() const {
  size_t res = 0;
  for (size_t i = 0; i < lock_map_stripes_.size(); ++i) {
    res += lock_map_stripes_[i]->committed_keys_map_.size();
  }
  return res;
}

Status TOTransactionDBImpl::AddToActiveTxns(const std::shared_ptr<ATN>& active_txn) {
  active_txns_.insert({active_txn->txn_id_, active_txn});
  return Status::OK();
}

TOTransaction* TOTransactionDBImpl::BeginTransaction(const WriteOptions& write_options,
                                     const TOTransactionOptions& txn_options) {
  std::shared_ptr<ATN> newActiveTxnNode = nullptr;
  TOTransaction* newTransaction = nullptr;
  {
    std::lock_guard<std::mutex> lock(active_txns_mutex_);

    TOTxnOptions totxn_option;
    totxn_option.max_write_batch_size = txn_options.max_write_batch_size;
    totxn_option.txn_snapshot = dbimpl_->GetSnapshot();
    totxn_option.log_ = info_log_;
    newActiveTxnNode = std::shared_ptr<ATN>(new ATN);
    newTransaction = new TOTransactionImpl(this, write_options, totxn_option, newActiveTxnNode);

    // Add the transaction to active txns
    newActiveTxnNode->txn_id_ = newTransaction->GetID();

    newActiveTxnNode->txn_snapshot = totxn_option.txn_snapshot;

    AddToActiveTxns(newActiveTxnNode);
  }
  
  ROCKS_LOG_DEBUG(info_log_, "TOTDB begin a txn id(%llu) snapshot(%llu) \n", newActiveTxnNode->txn_id_,
                  newActiveTxnNode->txn_snapshot->GetSequenceNumber());
  return newTransaction;

}

Status TOTransactionDBImpl::CheckWriteConflict(ColumnFamilyHandle* column_family,
                                        const Slice& key, 
										const TransactionID& txn_id,
                                        const RocksTimeStamp& readts) {
  //if first check the uc key and ck busy ,it will need remove uc key ,so we check commit key first
  auto stripe_num = GetStripe(key.ToString());
  assert(keys_mutex_.size() > stripe_num);
  std::lock_guard<std::mutex> lock(*keys_mutex_[stripe_num]);
  // Check whether some txn commits the key after current txn started
  // Check whether the commit ts of latest committed txn for key is less than my read ts
  Status s = committed_keys_.CheckKeyInLock(key, txn_id, readts, stripe_num);
  if (!s.ok()) {
	ROCKS_LOG_DEBUG(info_log_, "TOTDB txn id(%llu) key(%s) conflict ck \n", txn_id, key.ToString(true));
    return s;
  }

  // Check whether the key is in uncommitted keys
  // if not, add the key to uncommitted keys
  s = uncommitted_keys_.CheckKeyAndAddInLock(key, txn_id, stripe_num,
                                             max_conflict_bytes_, &current_conflict_bytes_);
  if (!s.ok()) {
    ROCKS_LOG_DEBUG(info_log_, "TOTDB txn id(%llu) key(%s) conflict uk \n", txn_id, key.ToString(true));
    return s;
  }

  return Status::OK();
}

void TOTransactionDBImpl::CleanCommittedKeys() {
  TransactionID txn = 0;
  RocksTimeStamp ts = 0;
  
  while(clean_job_.IsRunning()) {
    if (clean_job_.NeedToClean(&txn, &ts)) {
      for (size_t i = 0; i < num_stripes_; i++) {
        std::lock_guard<std::mutex> lock(*keys_mutex_[i]);
        CommittedLockMapStripe* stripe = committed_keys_.lock_map_stripes_.at(i);
    
        auto map_iter = stripe->committed_keys_map_.begin();
        while (map_iter != stripe->committed_keys_map_.end()) {
          auto history = map_iter->second;
          if (history.first <= txn && (history.second < ts || history.second == 0)) {
             auto ccbytes = current_conflict_bytes_.fetch_sub(
               map_iter->first.size() + sizeof(history.first) + sizeof(history.second), std::memory_order_relaxed);
             assert(ccbytes >= 0);
             (void)ccbytes;
             map_iter = stripe->committed_keys_map_.erase(map_iter);
          } else {
             map_iter ++;
          }
        }
      }
      clean_job_.FinishClean(txn, ts);
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  } 
  // End run clean thread
}

void TOTransactionDBImpl::StartBackgroundCleanThread() {
  clean_thread_ = std::thread([this] { CleanCommittedKeys(); });
}

void TOTransactionDBImpl::AdvanceTS(RocksTimeStamp* pMaxToCleanTs) {
  RocksTimeStamp maxToCleanTs = 0;

  {
    ReadLock rl(&ts_meta_mutex_);
    if (oldest_ts_ != nullptr) {
      maxToCleanTs = *oldest_ts_;
    }
  }

  {
    ReadLock rl(&read_ts_mutex_);
    for (auto it = read_q_.begin(); it != read_q_.end();) {
      if (it->second->state_.load() == TOTransaction::kStarted) {
        assert(it->second->read_ts_set_);
        maxToCleanTs = std::min(maxToCleanTs, it->second->read_ts_);
        break;
      }
      it++;
    }
  }
  *pMaxToCleanTs = maxToCleanTs;

  return;
}

Status TOTransactionDBImpl::AddReadQueue(const std::shared_ptr<ATN>& core,
                                         const RocksTimeStamp& ts,
                                         const uint32_t& round) {
  // the puzzle is the critical-area length of ts_meta_mutex_
  // between AddReadQueue and AddCommitQueue 

  RocksTimeStamp realTs = ts;
  ReadLock rl(&ts_meta_mutex_);
  if (oldest_ts_ != nullptr) {
    if (realTs < *oldest_ts_) {
      if (round == 0) { 
        return Status::InvalidArgument("read-ts smaller than oldest ts");
      }
      realTs = *oldest_ts_;
    } else {
      // otherwise, realTs >= *oldest_ts_
    }
  }

  // take care of the critical area, read_ts_mutex_ is within
  // ts_meta_mutex_
  WriteLock wl(&read_ts_mutex_);
  assert(!core->read_ts_set_);
  assert(core->state_.load() == TOTransaction::kStarted);
  // we have to clean commited/aboarted txns, right?
  // we just start from the beginning and clean until the first active txn
  // This is only a strategy and has nothing to do with the correctness
  // You may design other strategies.
  for (auto it = read_q_.begin(); it != read_q_.end();) {
    if (it->second->state_.load() == TOTransaction::kStarted) {
      break;
    }
    // TODO: add walk stat
    it = read_q_.erase(it);
  }
  core->read_ts_set_ = true;
  core->read_ts_ = realTs;
  read_q_.insert({{realTs, core->txn_id_}, core});
  return Status::OK();
}

Status TOTransactionDBImpl::AddCommitQueue(const std::shared_ptr<ATN>& core,
                                           const RocksTimeStamp& ts) {
  // we have no need to protect this action within
  // ts_meta_mutex_, because mongodb primary can guarantee AddCommitQueue
  // is within the hlc-allocator critical-area. And oldestTs is forked from
  // allcommitted-ts, so there is no way that pending-add ts is smaller
  // than oldestTs. But a check without ts_meta_mutex_ is not costive
  // and at some degree can guarantee corectness.

  {
    ReadLock rl(&ts_meta_mutex_);
    // see the comments above, we have no need to hold the rl too long.
    if (oldest_ts_ != nullptr && ts < *oldest_ts_) {
      return Status::InvalidArgument("commit-ts smaller than oldest ts");
    }
  }

  if (core->commit_ts_set_) {
    if (ts < core->first_commit_ts_) {
      return Status::InvalidArgument("commit-ts smaller than first-commit-ts");
    }
    if (ts < core->commit_ts_) {
      // in wt3.0, there is no restriction like this one.
      return Status::InvalidArgument("commit-ts must be monotonic in a txn");
    }
    core->commit_ts_ = ts;
    return Status::OK();
  }

  {
    // if we dont have a commit_ts, the ts set this time
    // shall be the first-commit-timestamp. And the commit-ts(es) set in
    // the following times will always be greater than first-commit-ts.
    // we publish first-commit-ts into the commit-queue, first-commit-ts
    // is fixed, never changed during a transaction.
    WriteLock wl(&commit_ts_mutex_);
    assert(!core->commit_ts_set_);
    assert(core->state_.load() == TOTransaction::kStarted);
    // we have to clean commited/aboarted txns, right?
    // we just start from the beginning and clean until the first active txn
    // This is only a strategy and has nothing to do with the correctness
    // You may design other strategies.
    for (auto it = commit_q_.begin(); it != commit_q_.end();) {
      if (it->second->state_.load() == TOTransaction::kStarted) {
        break;
      }
      // TODO: add walk stat
      it = commit_q_.erase(it);
    }
    assert(!core->commit_ts_set_);
    core->commit_ts_set_ = true;
    core->first_commit_ts_ = ts;
    core->commit_ts_ = ts;
    commit_q_.insert({{ts, core->txn_id_}, core});
  }
  return Status::OK();
}
Status TOTransactionDBImpl::CommitTransaction(std::shared_ptr<ATN> core,
                                              const std::set<std::string>& written_keys) {
  TransactionID maxToCleanTxnId = 0;
  RocksTimeStamp maxToCleanTs = 0;
  bool needClean = false;

  ROCKS_LOG_DEBUG(info_log_,
                "TOTDB start to commit txn id(%llu) commit ts(%llu)\n",
                core->txn_id_,
                core->commit_ts_);
  // Update Active Txns
  {
    std::lock_guard<std::mutex> lock(active_txns_mutex_);

    auto iter = active_txns_.find(core->txn_id_);
    assert(iter != active_txns_.end());
    assert(iter->second->state_.load() == TOTransaction::kStarted);
  
    iter->second->state_.store(TOTransaction::kCommitted);
    dbimpl_->ReleaseSnapshot(iter->second->txn_snapshot);
    core->commit_txn_id_ = TOTransactionImpl::GenTxnID();
	
    iter = active_txns_.erase(iter);

    if (core->txn_id_ > committed_max_txnid_) {
      committed_max_txnid_ = core->txn_id_;
    }
	
    if (iter == active_txns_.begin()) {
      needClean = true;
      if (!active_txns_.empty()) {
        maxToCleanTxnId = active_txns_.begin()->first - 1;
      } else {
        maxToCleanTxnId = committed_max_txnid_;
      }
    }
  }
  
  //it's okey to publish commit_ts a little later
  if (core->commit_ts_set_) {
    auto prev = committed_max_ts_.load(std::memory_order_relaxed);
    while (core->commit_ts_ > prev) {

      update_max_commit_ts_times_.fetch_add(1, std::memory_order_relaxed);
      if (committed_max_ts_.compare_exchange_strong(prev, core->commit_ts_)) {
        has_commit_ts_.store(true);
        break;
      }
      update_max_commit_ts_retries_.fetch_add(1, std::memory_order_relaxed);
      prev = committed_max_ts_.load(std::memory_order_relaxed);
    }
  } else {
    commit_without_ts_times_.fetch_add(1, std::memory_order_relaxed);
  }
  
  AdvanceTS(&maxToCleanTs);
  
  // txnid_ts_keys_map[{commit_txn_id, commit_ts}] = std::list<KeyModifyHistory::iterator>();
  // Move Uncommited keys for this txn to committed keys
  std::map<size_t, std::set<std::string>> stripe_keys_map;
  auto keys_iter = written_keys.begin();
  while (keys_iter != written_keys.end()) {
    auto stripe_num = GetStripe(*keys_iter); 
    if (stripe_keys_map.find(stripe_num) == stripe_keys_map.end()) {
      stripe_keys_map[stripe_num] = {};
    }
    stripe_keys_map[stripe_num].insert(std::move(*keys_iter));
    keys_iter++;
  }

  auto stripe_keys_iter = stripe_keys_map.begin();
  while (stripe_keys_iter != stripe_keys_map.end()) {
    std::lock_guard<std::mutex> lock(*keys_mutex_[stripe_keys_iter->first]);
    // the key in one txn insert to the CK with the max commit ts
    for (auto & key : stripe_keys_iter->second) {  
      committed_keys_.AddKeyInLock(key, core->commit_txn_id_,
                                   core->commit_ts_, stripe_keys_iter->first, &current_conflict_bytes_);
      uncommitted_keys_.RemoveKeyInLock(key, stripe_keys_iter->first, &current_conflict_bytes_);
    }

    stripe_keys_iter++;
  }

  ROCKS_LOG_DEBUG(info_log_, "TOTDB end commit txn id(%llu) cid(%llu) commit ts(%llu)\n",
                                                       core->txn_id_, core->commit_txn_id_, core->commit_ts_);
  // Clean committed keys
  if (needClean) {
    // Clean committed keys async 
    // Clean keys whose commited txnid <= maxToCleanTxnId
    // and committed ts < maxToCleanTs
    ROCKS_LOG_DEBUG(info_log_, "TOTDB going to clean txnid(%llu) ts(%llu) \n",
                  maxToCleanTxnId, maxToCleanTs);
    clean_job_.SetCleanInfo(maxToCleanTxnId, maxToCleanTs);
  }

  txn_commits_.fetch_add(1, std::memory_order_relaxed);
  if (core->read_ts_set_) {
    read_with_ts_times_.fetch_add(1, std::memory_order_relaxed);
  } else {
    read_without_ts_times_.fetch_add(1, std::memory_order_relaxed);
  }

  return Status::OK();
}

Status TOTransactionDBImpl::RollbackTransaction(std::shared_ptr<ATN> core,
                                     const std::set<std::string>& written_keys) {

  ROCKS_LOG_DEBUG(info_log_, "TOTDB start to rollback txn id(%llu) \n", core->txn_id_);
  // Remove txn for active txns
  bool needClean = false;
  TransactionID maxToCleanTxnId = 0;
  RocksTimeStamp maxToCleanTs = 0;
  {
    std::lock_guard<std::mutex> lock(active_txns_mutex_);

    auto iter = active_txns_.find(core->txn_id_);
    assert(iter != active_txns_.end());
    assert(iter->second->state_.load() == TOTransaction::kStarted);
    iter->second->state_.store(TOTransaction::kRollback);
    dbimpl_->ReleaseSnapshot(iter->second->txn_snapshot);
    iter = active_txns_.erase(iter); 
    
    if (iter == active_txns_.begin()) {
      needClean = true;
      if (!active_txns_.empty()) {
        maxToCleanTxnId = active_txns_.begin()->first - 1;
      } else {
        maxToCleanTxnId = committed_max_txnid_;
      }
    }
  }
  // Calculation the min clean ts between oldest and the read ts
  AdvanceTS(&maxToCleanTs);
  
  // Remove written keys from uncommitted keys

  std::map<size_t, std::set<std::string>> stripe_keys_map;
  auto keys_iter = written_keys.begin();
  while (keys_iter != written_keys.end()) {
    std::set<std::string> stripe_keys;
    auto stripe_num = GetStripe(*keys_iter); 
    if (stripe_keys_map.find(stripe_num) == stripe_keys_map.end()) {
      stripe_keys_map[stripe_num] = {};
    } 
    stripe_keys_map[stripe_num].insert(std::move(*keys_iter));

    keys_iter++;
  }

  auto stripe_keys_iter = stripe_keys_map.begin();
  while (stripe_keys_iter != stripe_keys_map.end()) {
    std::lock_guard<std::mutex> lock(*keys_mutex_[stripe_keys_iter->first]);

    for (auto & key : stripe_keys_iter->second) {  
      uncommitted_keys_.RemoveKeyInLock(key, stripe_keys_iter->first, &current_conflict_bytes_);
    }
    stripe_keys_iter++;
  }

  ROCKS_LOG_DEBUG(info_log_, "TOTDB end rollback txn id(%llu) \n", core->txn_id_);

  if (needClean) {
    ROCKS_LOG_DEBUG(info_log_, "TOTDB going to clean txnid(%llu) ts(%llu) \n",
                  maxToCleanTxnId, maxToCleanTs);
    clean_job_.SetCleanInfo(maxToCleanTxnId, maxToCleanTs);
  }

  txn_aborts_.fetch_add(1, std::memory_order_relaxed);
  if (core->read_ts_set_) {
    read_with_ts_times_.fetch_add(1, std::memory_order_relaxed);
  } else {
    read_without_ts_times_.fetch_add(1, std::memory_order_relaxed);
  }

  return Status::OK();
} 

Status TOTransactionDBImpl::SetTimeStamp(const TimeStampType& ts_type,
                                         const RocksTimeStamp& ts) {
  if (ts_type == kAllCommitted) {
    // TODO:
    // NOTE; actually, it should be called kCommittedTimestamp
    // and kCommittedTimestamp can be set backwards in wt.
    // But currently, we dont have this need.
    return Status::InvalidArgument("kAllCommittedTs can not set");
  }

  if (ts_type == kOldest) {
    // NOTE: here we must take lock, every txn's readTs-setting
    // has to be in the same critical area within the set of kOldest
    {
      WriteLock wl(&ts_meta_mutex_);
      if (oldest_ts_ != nullptr && *oldest_ts_ > ts) {
        return Status::InvalidArgument("oldestTs can not travel back");
      }
      oldest_ts_.reset(new RocksTimeStamp(ts));
    }
    auto pin_ts = ts;
    {
      ReadLock rl(&read_ts_mutex_);
      uint64_t walk_cnt = 0;
      for (auto it = read_q_.begin(); it != read_q_.end();) {
        if (it->second->state_.load() == TOTransaction::kStarted) {
          assert(it->second->read_ts_set_);
          pin_ts = std::min(pin_ts, it->second->read_ts_);
          break;
        }
        it++;
        walk_cnt++;
      }
      read_q_walk_len_sum_.fetch_add(read_q_.size(), std::memory_order_relaxed);
      read_q_walk_times_.fetch_add(walk_cnt, std::memory_order_relaxed);
    }
    dbimpl_->AdvancePinTs(pin_ts);
    ROCKS_LOG_DEBUG(info_log_, "TOTDB set TS type(%d) value(%llu)\n", ts_type, ts);
    return Status::OK();
  }

  return Status::InvalidArgument("invalid ts type");
}

Status TOTransactionDBImpl::QueryTimeStamp(const TimeStampType& ts_type, 
                                           RocksTimeStamp* timestamp) {
  if (ts_type == kAllCommitted) {
    if (!has_commit_ts_.load(/* seq_cst */)) {
      return Status::NotFound("not found");
    }
    auto tmp = committed_max_ts_.load(std::memory_order_relaxed);
    ReadLock rl(&commit_ts_mutex_);
    uint64_t walk_cnt = 0;
    for (auto it = commit_q_.begin(); it != commit_q_.end(); ++it) {
      if (it->second->state_.load(std::memory_order_relaxed) != TOTransaction::kStarted) {
        walk_cnt++;
        continue;
      }
      assert(it->second->commit_ts_set_);
      assert(it->second->first_commit_ts_ > 0);
      assert(it->second->commit_ts_ >= it->second->first_commit_ts_);
      tmp = std::min(tmp, it->second->first_commit_ts_-1);
      break;
    }
    commit_q_walk_len_sum_.fetch_add(commit_q_.size(), std::memory_order_relaxed);
    commit_q_walk_times_.fetch_add(walk_cnt, std::memory_order_relaxed);
    *timestamp = tmp;
    return Status::OK();
  }
  if (ts_type == kOldest) {
    // NOTE: query oldest is not a frequent thing, so I just
    // take the rlock
    ReadLock rl(&ts_meta_mutex_);
    if (oldest_ts_ == nullptr) {
      return Status::NotFound("not found");
    }
    *timestamp = *oldest_ts_;
    ROCKS_LOG_DEBUG(info_log_, "TOTDB query TS type(%llu) value(%llu) \n", ts_type, *timestamp);
    return Status::OK();
  }
  return Status::InvalidArgument("invalid ts_type");
}

Status TOTransactionDBImpl::Stat(TOTransactionStat* stat) {
  if (stat == nullptr) {
    return Status::InvalidArgument("can not accept null as input");
  }
  memset(stat, 0, sizeof(TOTransactionStat));
  stat->max_conflict_bytes = max_conflict_bytes_;
  stat->cur_conflict_bytes = current_conflict_bytes_.load(std::memory_order_relaxed);
  {
    std::vector<std::unique_lock<std::mutex>> lks;
    for (size_t i = 0; i < num_stripes_; i++) {
      lks.emplace_back(std::unique_lock<std::mutex>(*keys_mutex_[i]));
    }
    stat->uk_num = uncommitted_keys_.CountInLock();
    stat->ck_num = committed_keys_.CountInLock();
  }
  {
    std::lock_guard<std::mutex> lock(active_txns_mutex_);
    stat->alive_txns_num = active_txns_.size();
  }
  {
    ReadLock rl(&read_ts_mutex_);
    stat->read_q_num = read_q_.size();
    for (auto it = read_q_.begin(); it != read_q_.end(); it++) {
      assert(it->second->read_ts_set_);
      if (it->second->state_.load(std::memory_order_relaxed) == TOTransaction::kStarted) {
        stat->min_read_ts = it->second->read_ts_;
        break;
      }
    }
  }
  {
    ReadLock rl(&commit_ts_mutex_);
    stat->commit_q_num = commit_q_.size();
    for (auto it = commit_q_.begin(); it != commit_q_.end(); it++) {
      assert(it->second->commit_ts_set_);
      if (it->second->state_.load(std::memory_order_relaxed) == TOTransaction::kStarted) {
        stat->min_uncommit_ts = it->second->commit_ts_;
        break;
      }
    }
  }
  {
    ReadLock rl(&ts_meta_mutex_);
    stat->oldest_ts = oldest_ts_ == nullptr ? 0 : *oldest_ts_;
  }

  stat->max_commit_ts = has_commit_ts_.load() ? committed_max_ts_.load() : 0;
  stat->update_max_commit_ts_times = update_max_commit_ts_times_.load(std::memory_order_relaxed);
  stat->update_max_commit_ts_retries = update_max_commit_ts_retries_.load(std::memory_order_relaxed);
  stat->committed_max_txnid = committed_max_txnid_;
  stat->txn_commits = txn_commits_.load(std::memory_order_relaxed);
  stat->txn_aborts = txn_aborts_.load(std::memory_order_relaxed);
  stat->commit_without_ts_times = commit_without_ts_times_.load(std::memory_order_relaxed);
  stat->read_without_ts_times = read_without_ts_times_.load(std::memory_order_relaxed);
  stat->read_with_ts_times = read_with_ts_times_.load(std::memory_order_relaxed);
  stat->read_q_walk_len_sum = read_q_walk_len_sum_.load(std::memory_order_relaxed);
  stat->read_q_walk_times = read_q_walk_times_.load(std::memory_order_relaxed);
  stat->commit_q_walk_len_sum = commit_q_walk_len_sum_.load(std::memory_order_relaxed);
  stat->commit_q_walk_times = commit_q_walk_times_.load(std::memory_order_relaxed);
  return Status::OK();
}

Status TOTransactionDBImpl::BackgroundCleanJob::SetCleanInfo(const TransactionID& txn_id,
                                                             const RocksTimeStamp& time_stamp) {
  std::lock_guard<std::mutex> lock(thread_mutex_);
  txnid_ = txn_id;
  ts_ = time_stamp;
  return Status::OK();
}

bool TOTransactionDBImpl::BackgroundCleanJob::IsRunning() {
  std::lock_guard<std::mutex> lock(thread_mutex_);
  return thread_state_ == kRunning;
}

bool TOTransactionDBImpl::BackgroundCleanJob::NeedToClean(TransactionID* txn_id,
                                                          RocksTimeStamp* time_stamp) {
  std::lock_guard<std::mutex> lock(thread_mutex_);
  if (thread_state_ != kRunning) {
    return false;
  }
  *txn_id = txnid_;
  *time_stamp = ts_;
  return (txnid_ != 0);
}

void TOTransactionDBImpl::BackgroundCleanJob::FinishClean(const TransactionID& txn_id,
                                                          const RocksTimeStamp& time_stamp) {
  std::lock_guard<std::mutex> lock(thread_mutex_);
  if (txn_id == txnid_ && ts_ == time_stamp) {
    txnid_ = 0;
    ts_ = 0;
  }
}

void TOTransactionDBImpl::BackgroundCleanJob::StopThread() {
  std::lock_guard<std::mutex> lock(thread_mutex_);
  thread_state_ = kStopped;
}

}  //  namespace rocksdb

#endif  // ROCKSDB_LITE
