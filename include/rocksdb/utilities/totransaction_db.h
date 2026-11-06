#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include <utility>
#include <vector>

#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/totransaction.h"
#include "rocksdb/utilities/stackable_db.h"


namespace rocksdb {

//TimeStamp Ordering Transaction DB Options
#define DEFAULT_NUM_STRIPES 32

struct TOTransactionStat {
  size_t max_conflict_bytes;
  size_t cur_conflict_bytes;
  size_t uk_num;
  size_t ck_num;
  size_t alive_txns_num;
  size_t read_q_num;
  size_t commit_q_num;
  uint64_t oldest_ts;
  uint64_t min_read_ts;
  uint64_t max_commit_ts;
  uint64_t committed_max_txnid;
  uint64_t min_uncommit_ts;
  uint64_t update_max_commit_ts_times;
  uint64_t update_max_commit_ts_retries;
  uint64_t txn_commits;
  uint64_t txn_aborts;
  uint64_t commit_without_ts_times;
  uint64_t read_without_ts_times;
  uint64_t read_with_ts_times;
  uint64_t read_q_walk_len_sum;
  uint64_t read_q_walk_times;
  uint64_t commit_q_walk_len_sum;
  uint64_t commit_q_walk_times;
};

struct TOTransactionDBOptions {
  size_t num_stripes = DEFAULT_NUM_STRIPES;
  size_t max_conflict_check_bytes_size = 200*1024*1024;
};

enum TimeStampType {
    kOldest = 0,
    kStable = 1,
    kAllCommitted = 2,
    kTimeStampMax, 
};

//TimeStamp Ordering Transaction Options
struct TOTransactionOptions {
  size_t max_write_batch_size = 1000;
};

//TimeStamp Ordering Transaction Options
struct TOTxnOptions {
  size_t max_write_batch_size = 1000;
  const Snapshot* txn_snapshot = nullptr;
  Logger* log_ = nullptr;
};

class TOTransactionDB : public StackableDB {
  public:
  static Status Open(const Options& options,
                     const TOTransactionDBOptions& txn_db_options,
                     const std::string& dbname, TOTransactionDB** dbptr);

  static Status Open(const DBOptions& db_options,
                     const TOTransactionDBOptions& txn_db_options,
                     const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     TOTransactionDB** dbptr);

  // The lifecycle of returned pointer should be managed by the application level
  virtual TOTransaction* BeginTransaction(
      const WriteOptions& write_options,
      const TOTransactionOptions& txn_options) = 0;
 
  virtual Status SetTimeStamp(const TimeStampType& ts_type, const RocksTimeStamp& ts) = 0;

  virtual Status QueryTimeStamp(const TimeStampType& ts_type, RocksTimeStamp* timestamp) = 0;

  virtual Status Stat(TOTransactionStat* stat) = 0;
  //virtual Status Close();
  
  protected:
	//std::shared_ptr<Logger> info_log_ = nullptr;	
  // To Create an ToTransactionDB, call Open()
  explicit TOTransactionDB(DB* db) : StackableDB(db) {}
};

}  // namespace rocksdb

#endif

