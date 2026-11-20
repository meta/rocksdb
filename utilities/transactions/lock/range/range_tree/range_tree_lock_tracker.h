// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "util/mutexlock.h"
#include "utilities/transactions/lock/lock_tracker.h"
#include "utilities/transactions/pessimistic_transaction.h"

// Range Locking:
#include "lib/locktree/lock_request.h"
#include "lib/locktree/locktree.h"

namespace ROCKSDB_NAMESPACE {

class RangeTreeLockManager;

// Storage for locks that are currently held by a transaction.
//
// Locks are kept in toku::range_buffer because toku::locktree::release_locks()
// accepts that as an argument.
//
// Note: the list of locks may differ slighly from the contents of the lock
// tree, due to concurrency between lock acquisition, lock release, and lock
// escalation. See MDEV-18227 and RangeTreeLockManager::UnLock for details.
// This property is currently harmless.
//
// Append() and ReleaseLocks() are not thread-safe, as they are expected to be
// called only by the owner transaction. ReplaceLocks() is safe to call from
// other threads.
class RangeLockList {
 public:
  ~RangeLockList() { Clear(); }

  RangeLockList() : releasing_locks_(false) {}

  // No copying allowed
  RangeLockList(const RangeLockList&) = delete;
  RangeLockList& operator=(const RangeLockList&) = delete;
  RangeLockList(RangeLockList&&) = delete;
  RangeLockList& operator=(RangeLockList&&) = delete;

  void Append(ColumnFamilyId cf_id, const DBT* left_key, const DBT* right_key);
  void ReleaseLocks(RangeTreeLockManager* mgr, PessimisticTransaction* txn,
                    bool all_trx_locks);
  void ReplaceLocks(const toku::locktree* lt, const toku::range_buffer& buffer);

 private:
  void Clear() {
    for (const auto& it : buffers_) {
      it.second->destroy();
    }
    buffers_.clear();
  }

  std::unordered_map<ColumnFamilyId, std::shared_ptr<toku::range_buffer>>
      buffers_;
  port::Mutex mutex_;
  std::atomic<bool> releasing_locks_;
};

// A LockTracker-based object that is used together with RangeTreeLockManager.
class RangeTreeLockTracker : public LockTracker {
 public:
  RangeTreeLockTracker() : range_list_(nullptr) {}
  ~RangeTreeLockTracker() override = default;

  RangeTreeLockTracker(const RangeTreeLockTracker&) = delete;
  RangeTreeLockTracker& operator=(const RangeTreeLockTracker&) = delete;
  RangeTreeLockTracker(RangeTreeLockTracker&&) = delete;
  RangeTreeLockTracker& operator=(RangeTreeLockTracker&&) = delete;

  void Track(const PointLockRequest&) override;
  void Track(const RangeLockRequest&) override;

  bool IsPointLockSupported() const override {
    // This indicates that we don't implement GetPointLockStatus()
    return false;
  }
  bool IsRangeLockSupported() const override { return true; }

  // a Not-supported dummy implementation.
  UntrackStatus Untrack(const RangeLockRequest& /*lock_request*/) override {
    return UntrackStatus::NOT_TRACKED;
  }

  UntrackStatus Untrack(const PointLockRequest& /*lock_request*/) override {
    return UntrackStatus::NOT_TRACKED;
  }

  // "If this method is not supported, leave it as a no-op."
  void Merge(const LockTracker&) override {}

  // "If this method is not supported, leave it as a no-op."
  void Subtract(const LockTracker&) override {}

  void Clear() override;

  // "If this method is not supported, returns nullptr."
  LockTracker* GetTrackedLocksSinceSavePoint(
      const LockTracker&) const override {
    return nullptr;
  }

  PointLockStatus GetPointLockStatus(ColumnFamilyId column_family_id,
                                     const std::string& key) const override;

  // The return value is only used for tests
  uint64_t GetNumPointLocks() const override { return 0; }

  ColumnFamilyIterator* GetColumnFamilyIterator() const override {
    return nullptr;
  }

  KeyIterator* GetKeyIterator(
      ColumnFamilyId /*column_family_id*/) const override {
    return nullptr;
  }

  void ReleaseLocks(RangeTreeLockManager* mgr, PessimisticTransaction* txn,
                    bool all_trx_locks) {
    if (range_list_) {
      range_list_->ReleaseLocks(mgr, txn, all_trx_locks);
    }
  }

  void ReplaceLocks(const toku::locktree* lt,
                    const toku::range_buffer& buffer) {
    // range_list_ cannot be NULL here
    range_list_->ReplaceLocks(lt, buffer);
  }

 private:
  RangeLockList* getOrCreateList();
  std::unique_ptr<RangeLockList> range_list_;
};

class RangeTreeLockTrackerFactory : public LockTrackerFactory {
 public:
  static const RangeTreeLockTrackerFactory& Get() {
    static const RangeTreeLockTrackerFactory instance;
    return instance;
  }

  LockTracker* Create() const override { return new RangeTreeLockTracker(); }

 private:
  RangeTreeLockTrackerFactory() {}
};

}  // namespace ROCKSDB_NAMESPACE
