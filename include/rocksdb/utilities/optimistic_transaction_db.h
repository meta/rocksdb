//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/stackable_db.h"

namespace ROCKSDB_NAMESPACE {

class Transaction;

// Database with Transaction support.
//
// See optimistic_transaction.h and examples/transaction_example.cc

// Options to use when starting an Optimistic Transaction
struct OptimisticTransactionOptions {
  // Setting set_snapshot=true is the same as calling SetSnapshot().
  bool set_snapshot = false;

  // Should be set if the DB has a non-default comparator.
  // See comment in WriteBatchWithIndex constructor.
  const Comparator* cmp = BytewiseComparator();
};

enum class OccValidationPolicy {
  // Validate serially at commit stage, AFTER entering the write-group.
  // Isolation validation is processed single-threaded(since in the
  // write-group).
  // May suffer from high mutex contention, as per this link:
  // https://github.com/meta/rocksdb/issues/4402
  kValidateSerial = 0,
  // Validate parallelly before commit stage, BEFORE entering the write-group to
  // reduce mutex contention. Each txn acquires locks for its write-set
  // records in some well-defined order.
  kValidateParallel = 1
};

class OccLockBuckets {
 public:
  // Most details in internal derived class.
  // Users should not derive from this class.
  virtual ~OccLockBuckets() = default;

  virtual size_t ApproximateMemoryUsage() const = 0;

 private:
  friend class OccLockBucketsImplBase;
  OccLockBuckets() {}
};

// An object for sharing a pool of locks across DB instances.
//
// Making the locks cache-aligned avoids potential false sharing, at the
// potential cost of extra memory. The implementation has historically
// used cache_aligned = false.
std::shared_ptr<OccLockBuckets> MakeSharedOccLockBuckets(
    size_t bucket_count, bool cache_aligned = false);

struct OptimisticTransactionDBOptions {
  OccValidationPolicy validate_policy = OccValidationPolicy::kValidateParallel;

  // Number of striped/bucketed mutex locks for validating transactions.
  // Used on only if validate_policy == OccValidationPolicy::kValidateParallel
  // and shared_lock_buckets (below) is empty. Larger number potentially
  // reduces contention but uses more memory.
  uint32_t occ_lock_buckets = (1 << 20);

  // A pool of mutex locks for validating transactions. Can be shared among
  // DBs. Ignored if validate_policy != OccValidationPolicy::kValidateParallel.
  // If empty and validate_policy == OccValidationPolicy::kValidateParallel,
  // an OccLockBuckets will be created using the count in occ_lock_buckets.
  // See MakeSharedOccLockBuckets()
  std::shared_ptr<OccLockBuckets> shared_lock_buckets;
};

// Range deletions (including those in `WriteBatch`es passed to `Write()`) are
// incompatible with `OptimisticTransactionDB` and will return a non-OK `Status`
class OptimisticTransactionDB : public StackableDB {
 public:
  // Open an OptimisticTransactionDB similar to DB::Open().
  static Status Open(const Options& options, const std::string& dbname,
                     OptimisticTransactionDB** dbptr);

  static Status Open(const DBOptions& db_options, const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     OptimisticTransactionDB** dbptr);

  static Status Open(const DBOptions& db_options,
                     const OptimisticTransactionDBOptions& occ_options,
                     const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     OptimisticTransactionDB** dbptr);

  ~OptimisticTransactionDB() override {}

  // Starts a new Transaction.
  //
  // Caller is responsible for deleting the returned transaction when no
  // longer needed.
  //
  // If old_txn is not null, BeginTransaction will reuse this Transaction
  // handle instead of allocating a new one.  This is an optimization to avoid
  // extra allocations when repeatedly creating transactions.
  virtual Transaction* BeginTransaction(
      const WriteOptions& write_options,
      const OptimisticTransactionOptions& txn_options =
          OptimisticTransactionOptions(),
      Transaction* old_txn = nullptr) = 0;

  OptimisticTransactionDB(const OptimisticTransactionDB&) = delete;
  void operator=(const OptimisticTransactionDB&) = delete;
  OptimisticTransactionDB(OptimisticTransactionDB&&) = delete;
  void operator=(OptimisticTransactionDB&&) = delete;

 protected:
  // To Create an OptimisticTransactionDB, call Open()
  explicit OptimisticTransactionDB(std::unique_ptr<DB>&& db)
      : StackableDB(std::move(db)) {}
};

}  // namespace ROCKSDB_NAMESPACE
