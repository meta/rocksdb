// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/types.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "utilities/transactions/lock/lock_tracker.h"
#include "utilities/transactions/pessimistic_transaction.h"

namespace ROCKSDB_NAMESPACE {

class PessimisticTransactionDB;

class LockManager {
 public:
  virtual ~LockManager() = default;

  // Whether supports locking a specific key.
  virtual bool IsPointLockSupported() const = 0;

  // Whether supports locking a range of keys.
  virtual bool IsRangeLockSupported() const = 0;

  // Locks acquired through this LockManager should be tracked by
  // the LockTrackers created through the returned factory.
  virtual const LockTrackerFactory& GetLockTrackerFactory() const = 0;

  // Enable locking for the specified column family.
  // Caller should guarantee that this column family is not already enabled.
  virtual void AddColumnFamily(const ColumnFamilyHandle* cf) = 0;

  // Disable locking for the specified column family.
  // Caller should guarantee that this column family is no longer used.
  virtual void RemoveColumnFamily(const ColumnFamilyHandle* cf) = 0;

  // Attempt to lock a key or a key range.  If OK status is returned, the caller
  // is responsible for calling UnLock() on this key.
  virtual Status TryLock(PessimisticTransaction* txn,
                         ColumnFamilyId column_family_id,
                         const std::string& key, Env* env, bool exclusive) = 0;
  // The range [start, end] are inclusive at both sides.
  virtual Status TryLock(PessimisticTransaction* txn,
                         ColumnFamilyId column_family_id, const Endpoint& start,
                         const Endpoint& end, Env* env, bool exclusive) = 0;

  // Unlock a key or a range locked by TryLock().  txn must be the same
  // Transaction that locked this key.
  virtual void UnLock(PessimisticTransaction* txn, const LockTracker& tracker,
                      Env* env) = 0;
  virtual void UnLock(PessimisticTransaction* txn,
                      ColumnFamilyId column_family_id, const std::string& key,
                      Env* env) = 0;
  virtual void UnLock(PessimisticTransaction* txn,
                      ColumnFamilyId column_family_id, const Endpoint& start,
                      const Endpoint& end, Env* env) = 0;

  using PointLockStatus = std::unordered_multimap<ColumnFamilyId, KeyLockInfo>;
  virtual PointLockStatus GetPointLockStatus() = 0;

  using RangeLockStatus =
      std::unordered_multimap<ColumnFamilyId, RangeLockInfo>;
  virtual RangeLockStatus GetRangeLockStatus() = 0;

  virtual std::vector<DeadlockPath> GetDeadlockInfoBuffer() = 0;

  virtual void Resize(uint32_t new_size) = 0;
};

// LockManager should always be constructed through this factory method,
// instead of constructing through concrete implementations' constructor.
// Caller owns the returned pointer.
std::shared_ptr<LockManager> NewLockManager(PessimisticTransactionDB* db,
                                            const TransactionDBOptions& opt);

}  // namespace ROCKSDB_NAMESPACE
