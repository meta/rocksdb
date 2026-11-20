#pragma once
#ifndef ROCKSDB_LITE

#include "rocksdb/db.h"
#include "db/db_impl.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/totransaction.h"
#include "rocksdb/utilities/totransaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"


namespace rocksdb {

class TOTransactionDBImpl;

class TOTransactionImpl : public TOTransaction {
 public:
  struct ActiveTxnNode {
    TransactionID txn_id_;
    TransactionID commit_txn_id_;
    bool commit_ts_set_;
    RocksTimeStamp commit_ts_;
    RocksTimeStamp first_commit_ts_;
    bool read_ts_set_;
    RocksTimeStamp read_ts_;
    std::atomic<TOTransaction::TOTransactionState> state_;
    const Snapshot* txn_snapshot;
   public:
    ActiveTxnNode() 
        : txn_id_(0),
          commit_txn_id_(0),
          commit_ts_set_(false),
	      commit_ts_(0),
          first_commit_ts_(0),
          read_ts_set_(false),
          read_ts_(std::numeric_limits<uint64_t>::max()),
          state_(TOTransaction::kStarted) {
    }
  };

  TOTransactionImpl(TOTransactionDB* db,
              const WriteOptions& options, 
              const TOTxnOptions& txn_options,
              const std::shared_ptr<ActiveTxnNode>& core);

  virtual ~TOTransactionImpl();
	
  void Initialize();

  virtual Status SetCommitTimeStamp(const RocksTimeStamp& timestamp) override;

  virtual Status SetReadTimeStamp(const RocksTimeStamp& timestamp, const uint32_t& round) override;

  virtual Status GetReadTimeStamp(RocksTimeStamp* timestamp) const override;

  virtual Status Commit() override;

  virtual Status Rollback() override;

  virtual Status Get(ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     std::string* value) override;

  virtual Status Get(ReadOptions& options, const Slice& key,
                     std::string* value) override;

  virtual Iterator* GetIterator(ReadOptions& read_options) override;

  virtual Iterator* GetIterator(ReadOptions& read_options,
                                ColumnFamilyHandle* column_family) override;

  virtual Status Put(ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& value) override;

  virtual Status Put(const Slice& key, const Slice& value) override;

  virtual Status Delete(ColumnFamilyHandle* column_family, const Slice& key) override;

  virtual Status Delete(const Slice& key) override;

  virtual Status SetName(const TransactionName& name) override;

  virtual TransactionID GetID() const override { return txn_id_; };

  virtual WriteBatchWithIndex* GetWriteBatch() override;

  // Check write conflict. If there is no write conflict, add the key to uncommitted keys
  Status CheckWriteConflict(ColumnFamilyHandle* column_family, const Slice& key);

  // Generate a new unique transaction identifier
  static TransactionID GenTxnID();

 private:
  // Used to create unique ids for transactions.
  static std::atomic<TransactionID> txn_id_counter_;

  // Unique ID for this transaction
  TransactionID txn_id_;

  // Updated keys in this transaction
  std::set<std::string> writtenKeys_;

  DB* db_;
  DBImpl* db_impl_;
  TOTransactionDBImpl* txn_db_impl_;
  
  WriteOptions write_options_;
  TOTxnOptions txn_option_;

  const Comparator* cmp_;
  WriteBatchWithIndex write_batch_;

  std::shared_ptr<ActiveTxnNode> core_;  
};

} // namespace rocksdb
#endif
