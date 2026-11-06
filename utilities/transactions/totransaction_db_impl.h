//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include <array>


#include "rocksdb/db.h"
#include "db/db_impl.h"
#include "util/autovector.h"
#include "util/mutexlock.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/totransaction_db.h"
#include "util/murmurhash.h"
#include "utilities/transactions/totransaction_impl.h"

namespace rocksdb {

class TOTransactionDBImpl : public TOTransactionDB {
 public:
  TOTransactionDBImpl(DB* db, const TOTransactionDBOptions& txn_db_options, bool read_only)
              : TOTransactionDB(db),
                dbimpl_(reinterpret_cast<DBImpl*>(db)),
                read_only_(read_only),
                txn_db_options_(txn_db_options),
                num_stripes_(DEFAULT_NUM_STRIPES),
                committed_max_txnid_(0),
                current_conflict_bytes_(0),
                max_conflict_bytes_(1.1*txn_db_options.max_conflict_check_bytes_size),
                txn_commits_(0),
                txn_aborts_(0), 
                committed_max_ts_(0),
                has_commit_ts_(false),
                update_max_commit_ts_times_(0),
                update_max_commit_ts_retries_(0),
                commit_without_ts_times_(0),
                read_without_ts_times_(0),
                read_with_ts_times_(0),
                read_q_walk_times_(0),
                read_q_walk_len_sum_(0),
                commit_q_walk_times_(0),
                commit_q_walk_len_sum_(0),
                oldest_ts_(nullptr){
        if (max_conflict_bytes_ == 0) {
          // we preserve at least 100MB for conflict check
          max_conflict_bytes_ = 100*1024*1024;
        }
        info_log_ = dbimpl_->GetDBOptions().info_log.get();

        uncommitted_keys_.SetLogger(info_log_);
        committed_keys_.SetLogger(info_log_);
        active_txns_.clear();
        
        // Init default num_stripes
        num_stripes_  = (txn_db_options.num_stripes > 0)
                                 ? txn_db_options.num_stripes 
                                 : DEFAULT_NUM_STRIPES;

        
        uncommitted_keys_.lock_map_stripes_.reserve(num_stripes_);
        for (size_t i = 0; i < num_stripes_; i++) {
          UnCommittedLockMapStripe* stripe = new UnCommittedLockMapStripe();
          uncommitted_keys_.lock_map_stripes_.push_back(stripe);
        }

        committed_keys_.lock_map_stripes_.reserve(num_stripes_);
        for (size_t i = 0; i < num_stripes_; i++) {
          CommittedLockMapStripe* stripe = new CommittedLockMapStripe();
          committed_keys_.lock_map_stripes_.push_back(stripe);
        }

        keys_mutex_.reserve(num_stripes_);
        for (size_t i = 0; i < num_stripes_; i++) {
          std::mutex* key_mutex = new std::mutex();
          keys_mutex_.push_back(key_mutex);
        }
      }
				
  ~TOTransactionDBImpl() {
    // Clean resources
    clean_job_.StopThread();
    clean_thread_.join();

    {
      for (auto& it : uncommitted_keys_.lock_map_stripes_) {
        delete it;
      }
      uncommitted_keys_.lock_map_stripes_.clear();	

      for (auto& it : committed_keys_.lock_map_stripes_) {
        delete it;
      }
      committed_keys_.lock_map_stripes_.clear();
      
      for (auto& it : keys_mutex_) {
        delete it;
      }
      keys_mutex_.clear();
    }
    std::lock_guard<std::mutex> lock(active_txns_mutex_);
    active_txns_.clear();
  }

  void StartBackgroundCleanThread();

  void SetMaxConflictBytes(uint64_t bytes) {
    max_conflict_bytes_ = bytes;
  }

  virtual TOTransaction* BeginTransaction(const WriteOptions& write_options,
                                const TOTransactionOptions& txn_options) override;

  using ATN = TOTransactionImpl::ActiveTxnNode;
  Status CommitTransaction(std::shared_ptr<ATN> core,
                           const std::set<std::string>& written_keys);

  Status RollbackTransaction(std::shared_ptr<ATN> core,
                             const std::set<std::string>& written_keys);
	 
  Status SetTimeStamp(const TimeStampType& ts_type, const RocksTimeStamp& ts) override;
 
  Status QueryTimeStamp(const TimeStampType& ts_type, RocksTimeStamp* timestamp) override;

  Status Stat(TOTransactionStat* stat) override;

  Status CheckWriteConflict(ColumnFamilyHandle* column_family,
                            const Slice& key, 
							const TransactionID& txn_id,
                            const RocksTimeStamp& readts); 

  Status AddCommitQueue(const std::shared_ptr<ATN>& core,
                        const RocksTimeStamp& ts);

  Status AddReadQueue(const std::shared_ptr<ATN>& core,
                      const RocksTimeStamp& ts,
                      const uint32_t& round);

  void AdvanceTS(RocksTimeStamp* maxToCleanTs);
 
  void CleanCommittedKeys();

  bool IsReadOnly() const { return read_only_; }

  // Committed key, first commit txnid, second commit ts
  typedef std::pair<TransactionID, RocksTimeStamp> KeyModifyHistory;

 protected:
  DBImpl* dbimpl_;
  bool read_only_;
  const TOTransactionDBOptions txn_db_options_;
  Logger* info_log_ = nullptr;
  size_t num_stripes_;
  TransactionID committed_max_txnid_;
  std::atomic<int64_t> current_conflict_bytes_;
  int64_t max_conflict_bytes_;
  std::atomic<uint64_t> txn_commits_;
  std::atomic<uint64_t> txn_aborts_;

  class BackgroundCleanJob {
    std::mutex thread_mutex_;
    TransactionID txnid_;
    RocksTimeStamp ts_;
    
    enum ThreadState {
      kRunning,
      kStopped
    };

    ThreadState thread_state_;
   public:
    BackgroundCleanJob()
      :txnid_(0),ts_(0) {
      thread_state_ = kRunning;
    }

    ~BackgroundCleanJob() {
    }

    Status SetCleanInfo(const TransactionID& txn_id,
                        const RocksTimeStamp& time_stamp);

    bool IsRunning();

    bool NeedToClean(TransactionID* txn_id, 
                     RocksTimeStamp* time_stamp);

    void FinishClean(const TransactionID& txn_id,
                     const RocksTimeStamp& time_stamp);

    void StopThread();
  };
 private:

  using TSTXN = std::pair<RocksTimeStamp, TransactionID>;
  // Add txn to active txns
  Status AddToActiveTxns(const std::shared_ptr<ATN>& active_txn);

  void RemoveUncommittedKeysOnCleanup(const std::set<std::string>& written_keys);

  // Active txns
  std::mutex active_txns_mutex_;
  std::map<TransactionID, std::shared_ptr<ATN>> active_txns_;

  // txns sorted by {commit_ts, txnid}
  port::RWMutex commit_ts_mutex_;
  std::map<TSTXN, std::shared_ptr<ATN>> commit_q_;

  // txns sorted by {read_ts, txnid}
  port::RWMutex read_ts_mutex_;
  std::map<TSTXN, std::shared_ptr<ATN>> read_q_;

  struct UnCommittedLockMapStripe {
    std::map<std::string, TransactionID>  uncommitted_keys_map_;
  };
  
  size_t GetStripe(const std::string& key) const {
    assert(num_stripes_ > 0);
    static murmur_hash hash;
    size_t stripe = hash(key) % num_stripes_;
    return stripe;
  }
  // Uncommitted keys
  struct UnCommittedKeys {
    std::vector<UnCommittedLockMapStripe*> lock_map_stripes_;
    Logger* info_log_;
   public:
    // Remove key from uncommitted keys
    void SetLogger(Logger* info_log) {
      info_log_ = info_log;
    }
    Status RemoveKeyInLock(const Slice& key, const size_t& stripe_num,
                           std::atomic<int64_t>* mem_usage);
    // Check write conflict and add the key to uncommitted keys
    Status CheckKeyAndAddInLock(const Slice& key, 
                          const TransactionID& txn_id,
                          const size_t& stripe_num,
                          const size_t& max_mem_usage,
                          std::atomic<int64_t>* mem_usage); 

    size_t CountInLock() const;
  };

  struct CommittedLockMapStripe {
    //std::mutex map_mutex_;
    std::map<std::string, KeyModifyHistory> committed_keys_map_;
  };

  struct CommittedKeys {
    std::vector<CommittedLockMapStripe*> lock_map_stripes_;
    Logger* info_log_;
   public:
    void SetLogger(Logger* info_log) {
      info_log_ = info_log;
    }
    // Add key to committed keys
    Status AddKeyInLock(const Slice& key, 
                  const TransactionID& commit_txn_id, 
                  const RocksTimeStamp& commit_ts,
                  const size_t& stripe_num,
                  std::atomic<int64_t>* mem_usage);

    // Remove key from committed keys
    Status RemoveKeyInLock(const Slice& key, 
                     const TransactionID& txn_id,
                     const size_t& stripe_num,
                     std::atomic<int64_t>* mem_usage);

    // Check write conflict
    Status CheckKeyInLock(const Slice& key, 
                    const TransactionID& txn_id,
                    const RocksTimeStamp& timestamp,
                    const size_t& stripe_num);

    size_t CountInLock() const;
  };

  std::vector<std::mutex*> keys_mutex_;
 
  UnCommittedKeys uncommitted_keys_;

  CommittedKeys committed_keys_;

  BackgroundCleanJob clean_job_;

  std::thread clean_thread_;

  // NOTE(wolfkdy): commit_ts_ is not protected by ts_meta_mutex_
  // remember to publish commit_ts_ before has_commit_ts_
  std::atomic<RocksTimeStamp> committed_max_ts_;
  std::atomic<bool> has_commit_ts_;
  std::atomic<uint64_t> update_max_commit_ts_times_;
  std::atomic<uint64_t> update_max_commit_ts_retries_;
  std::atomic<uint64_t> commit_without_ts_times_;
  std::atomic<uint64_t> read_without_ts_times_;
  std::atomic<uint64_t> read_with_ts_times_;
  std::atomic<uint64_t> read_q_walk_times_;
  std::atomic<uint64_t> read_q_walk_len_sum_;
  std::atomic<uint64_t> commit_q_walk_times_;
  std::atomic<uint64_t> commit_q_walk_len_sum_;

  // TODO(wolfkdy): use optional<>
  port::RWMutex ts_meta_mutex_;
  // protected by ts_meta_mutex_
  std::unique_ptr<RocksTimeStamp> oldest_ts_;
  // protected by ts_meta_mutex_
  std::unique_ptr<RocksTimeStamp> pinned_ts_;
};

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
