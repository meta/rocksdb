// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/transaction_db.h"

namespace ROCKSDB_NAMESPACE {

// Request for locking a single key.
struct PointLockRequest {
  // The id of the key's column family.
  ColumnFamilyId column_family_id = 0;
  // The key to lock.
  std::string key;
  // The sequence number from which there is no concurrent update to key.
  SequenceNumber seq = 0;
  // Whether the lock is acquired only for read.
  bool read_only = false;
  // Whether the lock is in exclusive mode.
  bool exclusive = true;
};

// Request for locking a range of keys.
struct RangeLockRequest {
  // The id of the key's column family.
  ColumnFamilyId column_family_id = 0;

  // The range to be locked
  Endpoint start_endp;
  Endpoint end_endp;
};

struct PointLockStatus {
  // Whether the key is locked.
  bool locked = false;
  // Whether the key is locked in exclusive mode.
  bool exclusive = true;
  // The sequence number in the tracked PointLockRequest.
  SequenceNumber seq = 0;
};

// Return status when calling LockTracker::Untrack.
enum class UntrackStatus {
  // The lock is not tracked at all, so no lock to untrack.
  NOT_TRACKED,
  // The lock is untracked but not removed from the tracker.
  UNTRACKED,
  // The lock is removed from the tracker.
  REMOVED,
};

// Tracks the lock requests.
// In PessimisticTransaction, it tracks the locks acquired through LockMgr;
// In OptimisticTransaction, since there is no LockMgr, it tracks the lock
// intention. Not thread-safe.
class LockTracker {
 public:
  virtual ~LockTracker() = default;

  // Whether supports locking a specific key.
  virtual bool IsPointLockSupported() const = 0;

  // Whether supports locking a range of keys.
  virtual bool IsRangeLockSupported() const = 0;

  // Tracks the acquirement of a lock on key.
  //
  // If this method is not supported, leave it as a no-op.
  virtual void Track(const PointLockRequest& /*lock_request*/) = 0;

  // Untracks the lock on a key.
  // seq and exclusive in lock_request are not used.
  //
  // If this method is not supported, leave it as a no-op and
  // returns NOT_TRACKED.
  virtual UntrackStatus Untrack(const PointLockRequest& /*lock_request*/) = 0;

  // Counterpart of Track(const PointLockRequest&) for RangeLockRequest.
  virtual void Track(const RangeLockRequest& /*lock_request*/) = 0;

  // Counterpart of Untrack(const PointLockRequest&) for RangeLockRequest.
  virtual UntrackStatus Untrack(const RangeLockRequest& /*lock_request*/) = 0;

  // Merges lock requests tracked in the specified tracker into the current
  // tracker.
  //
  // E.g. for point lock, if a key in tracker is not yet tracked,
  // track this new key; otherwise, merge the tracked information of the key
  // such as lock's exclusiveness, read/write statistics.
  //
  // If this method is not supported, leave it as a no-op.
  //
  // REQUIRED: the specified tracker must be of the same concrete class type as
  // the current tracker.
  virtual void Merge(const LockTracker& /*tracker*/) = 0;

  // This is a reverse operation of Merge.
  //
  // E.g. for point lock, if a key exists in both current and the sepcified
  // tracker, then subtract the information (such as read/write statistics) of
  // the key in the specified tracker from the current tracker.
  //
  // If this method is not supported, leave it as a no-op.
  //
  // REQUIRED:
  // The specified tracker must be of the same concrete class type as
  // the current tracker.
  // The tracked locks in the specified tracker must be a subset of those
  // tracked by the current tracker.
  virtual void Subtract(const LockTracker& /*tracker*/) = 0;

  // Clears all tracked locks.
  virtual void Clear() = 0;

  // Gets the new locks (excluding the locks that have been tracked before the
  // save point) tracked since the specified save point, the result is stored
  // in an internally constructed LockTracker and returned.
  //
  // save_point_tracker is the tracker used by a SavePoint to track locks
  // tracked after creating the SavePoint.
  //
  // The implementation should document whether point lock, or range lock, or
  // both are considered in this method.
  // If this method is not supported, returns nullptr.
  //
  // REQUIRED:
  // The save_point_tracker must be of the same concrete class type as the
  // current tracker.
  // The tracked locks in the specified tracker must be a subset of those
  // tracked by the current tracker.
  virtual LockTracker* GetTrackedLocksSinceSavePoint(
      const LockTracker& /*save_point_tracker*/) const = 0;

  // Gets lock related information of the key.
  //
  // If point lock is not supported, always returns LockStatus with
  // locked=false.
  virtual PointLockStatus GetPointLockStatus(
      ColumnFamilyId /*column_family_id*/,
      const std::string& /*key*/) const = 0;

  // Gets number of tracked point locks.
  //
  // If point lock is not supported, always returns 0.
  virtual uint64_t GetNumPointLocks() const = 0;

  class ColumnFamilyIterator {
   public:
    virtual ~ColumnFamilyIterator() = default;

    // Whether there are remaining column families.
    virtual bool HasNext() const = 0;

    // Gets next column family id.
    //
    // If HasNext is false, calling this method has undefined behavior.
    virtual ColumnFamilyId Next() = 0;
  };

  // Gets an iterator for column families.
  //
  // Returned iterator must not be nullptr.
  // If there is no column family to iterate,
  // returns an empty non-null iterator.
  // Caller owns the returned pointer.
  virtual ColumnFamilyIterator* GetColumnFamilyIterator() const = 0;

  class KeyIterator {
   public:
    virtual ~KeyIterator() = default;

    // Whether there are remaining keys.
    virtual bool HasNext() const = 0;

    // Gets the next key.
    //
    // If HasNext is false, calling this method has undefined behavior.
    virtual const std::string& Next() = 0;
  };

  // Gets an iterator for keys with tracked point locks in the column family.
  //
  // The column family must exist.
  // Returned iterator must not be nullptr.
  // Caller owns the returned pointer.
  virtual KeyIterator* GetKeyIterator(
      ColumnFamilyId /*column_family_id*/) const = 0;
};

// LockTracker should always be constructed through this factory.
// Each LockManager owns a LockTrackerFactory.
class LockTrackerFactory {
 public:
  // Caller owns the returned pointer.
  virtual LockTracker* Create() const = 0;
  virtual ~LockTrackerFactory() = default;
};

}  // namespace ROCKSDB_NAMESPACE
